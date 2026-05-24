package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type pendingEntry struct {
	ch        chan net.Conn
	createdAt time.Time
}

var (
	pendingMu  sync.Mutex
	pendingMap = make(map[string]*pendingEntry)
)

func registerPending(token string) chan net.Conn {
	ch := make(chan net.Conn, 1)
	pendingMu.Lock()
	pendingMap[token] = &pendingEntry{ch: ch, createdAt: time.Now()}
	pendingMu.Unlock()
	return ch
}

func takePending(token string) chan net.Conn {
	pendingMu.Lock()
	entry := pendingMap[token]
	if entry != nil {
		delete(pendingMap, token)
	}
	pendingMu.Unlock()
	if entry != nil {
		return entry.ch
	}
	return nil
}

func removePending(token string) {
	pendingMu.Lock()
	delete(pendingMap, token)
	pendingMu.Unlock()
}

func init() {
	go func() {
		for {
			time.Sleep(30 * time.Second)
			pendingMu.Lock()
			now := time.Now()
			for token, entry := range pendingMap {
				if now.Sub(entry.createdAt) > 60*time.Second {
					delete(pendingMap, token)
				}
			}
			pendingMu.Unlock()
		}
	}()
}

// StartTCPProxy 启动 TCP 代理监听（autossh 模式：每连接一条隧道）
func (h *APIHandler) StartTCPProxy(port int, bindAddr string, table *manager.ClientTable, token string) {
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		alog.Error(alog.CatProxy, "listen failed", "error", err, "port", port)
		return
	}
	defer ln.Close()

	key := table.TunnelKey(port)

	proxy := table.GetProxy(port)
	if proxy != nil {
		table.AddProxy(&manager.ProxyInfo{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			Listener:   ln,
		})
	}

	alog.Info(alog.CatProxy, "TCP proxy listening", "port", port, "client_id", table.ClientID())

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		go h.handlePublicConn(conn, key, table)
	}
}

func (h *APIHandler) handlePublicConn(conn net.Conn, key string, table *manager.ClientTable) {
	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatProxy, "public connection, requesting tunnel", "remote", remoteAddr, "key", key)

	tunnelToken := genRandomToken(16)
	pendingCh := registerPending(tunnelToken)
	defer removePending(tunnelToken)
	defer conn.Close()

	reqData, err := json.Marshal(model.TunnelRequestData{
		Key:   key,
		Token: tunnelToken,
	})
	if err != nil {
		alog.Error(alog.CatProxy, "marshal tunnel_request failed", "error", err)
		return
	}

	notify := model.WSMessage{
		Type: "tunnel_request",
		Data: string(reqData),
	}
	if err := table.Conn().WriteJSON(&notify); err != nil {
		alog.Error(alog.CatProxy, "send tunnel_request failed", "remote", remoteAddr, "error", err)
		return
	}

	select {
	case tunnelConn := <-pendingCh:
		if _, err := tunnelConn.Write([]byte{0x01}); err != nil {
			alog.Error(alog.CatProxy, "send tunnel ack failed", "error", err)
			tunnelConn.Close()
			return
		}
		alog.Info(alog.CatProxy, "tunnel paired", "remote", remoteAddr, "key", key)
		pipeBidir(conn, tunnelConn)
	case <-time.After(60 * time.Second):
		alog.Warn(alog.CatProxy, "tunnel pairing timeout", "remote", remoteAddr, "key", key)
	}
}

// AcceptTunnel 接受客户端隧道连接并配对公网连接
func (h *APIHandler) AcceptTunnel(tunnelConn net.Conn, token string) {
	ch := takePending(token)
	if ch == nil {
		alog.Warn(alog.CatProxy, "no pending public connection for tunnel token")
		tunnelConn.Close()
		return
	}

	select {
	case ch <- tunnelConn:
	case <-time.After(5 * time.Second):
		alog.Warn(alog.CatProxy, "public connection already gone, dropping tunnel")
		tunnelConn.Close()
	}
}

func pipeBidir(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer a.Close()
		io.Copy(a, b)
	}()

	go func() {
		defer wg.Done()
		defer b.Close()
		io.Copy(b, a)
	}()

	wg.Wait()
}

func genRandomToken(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}
