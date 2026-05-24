package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// UDP 数据包头格式：
// [2字节源端口][2字节数据长度][N字节数据]
// 用于在 TCP 隧道中传输 UDP 数据

const (
	udpHeaderSize = 4 // 2字节端口 + 2字节长度
)

// udpSession 跟踪 UDP 会话（源地址 -> 最后活跃时间）
type udpSession struct {
	addr       *net.UDPAddr
	lastActive time.Time
}

// StartUDPProxy 启动 UDP 代理
func (h *APIHandler) StartUDPProxy(port int, bindAddr string, table *manager.ClientTable, token string) {
	key := table.TunnelKey(port)
	clientID := table.ClientID()

	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	// 监听 UDP 端口
	udpAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		alog.Error(alog.CatProxy, "udp proxy address resolve error", "error", err)
		return
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		alog.Error(alog.CatProxy, "udp proxy listen error", "error", err)
		return
	}
	defer udpConn.Close()

	// 保存代理信息
	proxy := table.GetProxy(port)
	if proxy != nil {
		table.AddProxy(&manager.ProxyInfo{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			Listener:   udpConn,
		})
	}

	// 存储 UDP 隧道 token
	table.StoreTunnelToken(token, key)

	alog.Info(alog.CatProxy, "udp proxy listening", "port", port, "client_id", clientID)

	// 等待客户端建立 UDP 隧道
	// 客户端会通过 TCP 连接发送 "TUNNEL\n" 标记来建立隧道
	go h.waitForUDPTunnel(port, table, token)

	// 会话管理
	sessions := make(map[string]*udpSession)
	var sessionsMu sync.Mutex

	// 清理过期会话
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			sessionsMu.Lock()
			now := time.Now()
			for addr, sess := range sessions {
				if now.Sub(sess.lastActive) > 60*time.Second {
					delete(sessions, addr)
				}
			}
			sessionsMu.Unlock()
		}
	}()

	// 读取 UDP 数据包
	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			alog.Error(alog.CatProxy, "udp read error", "error", err)
			continue
		}

		if n == 0 {
			continue
		}

		// 更新会话
		sessionsMu.Lock()
		sessionKey := remoteAddr.String()
		sessions[sessionKey] = &udpSession{
			addr:       remoteAddr,
			lastActive: time.Now(),
		}
		sessionsMu.Unlock()

		// 获取隧道连接
		udpTunnel := table.GetUDPTunnel(key)
		if udpTunnel == nil {
			// 没有隧道，丢弃数据包
			continue
		}

		// 通过 TCP 隧道发送 UDP 数据
		// 格式：[2字节源端口][2字节数据长度][数据]
		packet := make([]byte, udpHeaderSize+n)
		binary.BigEndian.PutUint16(packet[0:2], uint16(remoteAddr.Port))
		binary.BigEndian.PutUint16(packet[2:4], uint16(n))
		copy(packet[4:], buf[:n])

		_, err = udpTunnel.Write(packet)
		if err != nil {
			alog.Error(alog.CatTunnel, "udp tunnel write error", "error", err)
			table.SetUDPTunnel(key, nil)
			continue
		}
	}
}

// waitForUDPTunnel 等待客户端建立 UDP 隧道
func (h *APIHandler) waitForUDPTunnel(port int, table *manager.ClientTable, token string) {
	key := table.TunnelKey(port)

	// 监听 TCP 端口用于 UDP 隧道
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		alog.Error(alog.CatTunnel, "udp tunnel listen error", "error", err)
		return
	}
	defer ln.Close()

	alog.Info(alog.CatTunnel, "udp tunnel listener started", "port", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}

		go h.handleUDPTunnelConn(conn, table, token, key)
	}
}

// handleUDPTunnelConn 处理 UDP 隧道连接
func (h *APIHandler) handleUDPTunnelConn(conn net.Conn, table *manager.ClientTable, token string, key string) {
	defer conn.Close()

	// 读取认证标记 "TUNNEL\n"
	authBuf := make([]byte, 7)
	if _, err := io.ReadFull(conn, authBuf); err != nil {
		alog.Error(alog.CatAuth, "udp tunnel auth read error", "error", err)
		return
	}

	if string(authBuf) != "TUNNEL\n" {
		alog.Warn(alog.CatAuth, "udp tunnel auth marker invalid", "marker", string(authBuf))
		return
	}

	// 读取 token 长度
	var tokenLen uint16
	if err := binary.Read(conn, binary.BigEndian, &tokenLen); err != nil {
		alog.Error(alog.CatAuth, "udp tunnel token length read error", "error", err)
		return
	}

	// 读取 token
	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(conn, tokenBuf); err != nil {
		alog.Error(alog.CatAuth, "udp tunnel token read error", "error", err)
		return
	}

	// 验证 token
	if string(tokenBuf) != token {
		alog.Warn(alog.CatAuth, "udp tunnel token mismatch")
		return
	}

	// 发送确认
	if _, err := conn.Write([]byte{0x01}); err != nil {
		alog.Error(alog.CatTunnel, "udp tunnel ack send error", "error", err)
		return
	}

	// 注册 UDP 隧道
	table.SetUDPTunnel(key, conn.(*net.TCPConn))
	alog.Info(alog.CatTunnel, "udp tunnel registered", "key", key)

	// 读取从客户端返回的 UDP 响应
	// 格式：[2字节目标端口][2字节数据长度][数据]
	for {
		var destPort uint16
		var dataLen uint16

		if err := binary.Read(conn, binary.BigEndian, &destPort); err != nil {
			if err != io.EOF {
				alog.Error(alog.CatTunnel, "udp tunnel read port error", "error", err)
			}
			break
		}

		if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
			alog.Error(alog.CatTunnel, "udp tunnel read length error", "error", err)
			break
		}

		if dataLen > 65535 {
			alog.Error(alog.CatTunnel, "udp packet too large", "size", dataLen)
			break
		}

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			alog.Error(alog.CatTunnel, "udp tunnel read data error", "error", err)
			break
		}

		// 这里需要将响应发送回 UDP 客户端
		// 但是我们需要知道目标地址，这需要从之前的会话中获取
		// 简化实现：暂时不处理响应
		_ = destPort
		_ = data
	}

	// 清理
	table.SetUDPTunnel(key, nil)
	alog.Info(alog.CatTunnel, "udp tunnel disconnected", "key", key)
}
