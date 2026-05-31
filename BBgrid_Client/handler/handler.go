// Package handler 处理从服务器接收的消息，并管理 TCP 和 WebSocket 隧道的生命周期。
package handler

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/mux"
	"BBgrid/common/proto"
	"BBgrid/common/wsconn"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MessageSender 通过 WebSocket 连接发送 JSON 消息。
type MessageSender interface {
	WriteJSON(v interface{}) error
}

// Config 隧道连接配置。
type Config struct {
	ClientID       string
	UseHTTP        bool
	Insecure       bool
	SNIOverride    string
	OriginOverride string
	UDPTunnelKey   string
}

// Handler 处理入站服务器命令并管理隧道实例。
type Handler struct {
	cfg        Config
	sender     MessageSender
	baseCtx    context.Context
	baseCancel context.CancelFunc
	tunnelMu   sync.Mutex
	tunnelCtxs map[string]context.CancelFunc
	relay      *relayManager

	proxyMu        sync.RWMutex
	proxyInfo      map[string]*model.CommandData
	tunnelContexts map[string]context.Context

	// 更新状态
	updateMu      sync.Mutex
	updateData    []byte
	updateMD5     string
	updateSize    int
	updateChunksN int
}

// New 创建新的 Handler。
func New(cfg Config) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		cfg:            cfg,
		baseCtx:        ctx,
		baseCancel:     cancel,
		tunnelCtxs:     make(map[string]context.CancelFunc),
		proxyInfo:      make(map[string]*model.CommandData),
		tunnelContexts: make(map[string]context.Context),
	}
	relayMgr := newRelayManager(cfg, ctx)
	h.relay = relayMgr
	return h
}

// SetSender 设置消息发送器（通常是 conn.Connection）。
func (h *Handler) SetSender(sender MessageSender) {
	h.sender = sender
	if h.relay != nil {
		h.relay.SetSender(sender)
	}
}

// Stop 取消所有正在运行的隧道。
func (h *Handler) Stop() {
	h.baseCancel()
	if h.relay != nil {
		h.relay.Close()
	}
}

// Handle 根据类型分发入站服务器消息。
func (h *Handler) Handle(msg *model.WSMessage) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatSystem, "PANIC in handler.Handle", "error", r)
		}
	}()
	switch msg.Type {
	case "proxy":
		h.handleProxy(msg.Data)
	case "proxy_closed":
		h.handleProxyClosed(msg.Data)
	case "proxy_outbound":
		h.handleProxyOutbound(msg.Data)
	case "tunnel_request":
		h.handleTunnelRequest(msg.Data)
	case "ping":
		h.handlePing(msg.Data)
	case "relay_signal":
		if h.relay != nil {
			h.relay.handleRelaySignal(msg.Data)
		}
	case "relay_closed":
		if h.relay != nil {
			h.relay.handleRelayClosed(msg.Data)
		}
	case "update_start":
		h.handleUpdateStart(msg.Data)
	case "update_chunk":
		h.handleUpdateChunk(msg.Data)
	case "update_end":
		h.handleUpdateEnd()
	}
}

func (h *Handler) registerTunnel(key string, cancel context.CancelFunc) {
	h.tunnelMu.Lock()
	h.tunnelCtxs[key] = cancel
	h.tunnelMu.Unlock()
}

func (h *Handler) stopTunnel(key string) {
	h.tunnelMu.Lock()
	if cancel, ok := h.tunnelCtxs[key]; ok {
		cancel()
		delete(h.tunnelCtxs, key)
	}
	h.tunnelMu.Unlock()
}

func (h *Handler) handleUpdateStart(data interface{}) {
	m, ok := data.(map[string]interface{})
	if !ok {
		alog.Warn(alog.CatUpdate, "update_start: invalid data")
		return
	}

	h.updateMu.Lock()
	defer h.updateMu.Unlock()

	md5Val, _ := m["md5"].(string)
	sizeVal, _ := m["size"].(float64)
	chunksVal, _ := m["chunks"].(float64)

	h.updateMD5 = md5Val
	h.updateSize = int(sizeVal)
	h.updateChunksN = int(chunksVal)
	h.updateData = make([]byte, 0, h.updateSize)

	alog.Info(alog.CatUpdate, "收到更新开始", "md5", h.updateMD5, "size", h.updateSize, "chunks", h.updateChunksN)
}

func (h *Handler) handleUpdateChunk(data interface{}) {
	m, ok := data.(map[string]interface{})
	if !ok {
		alog.Warn(alog.CatUpdate, "update_chunk: invalid data")
		return
	}

	dataStr, _ := m["data"].(string)
	chunk, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		alog.Error(alog.CatUpdate, "update_chunk: decode error", "error", err)
		return
	}

	h.updateMu.Lock()
	if h.updateSize == 0 {
		h.updateMu.Unlock()
		alog.Warn(alog.CatUpdate, "update_chunk: update_start not received, ignoring")
		return
	}
	if len(h.updateData)+len(chunk) > h.updateSize {
		h.updateMu.Unlock()
		alog.Warn(alog.CatUpdate, "update_chunk: data exceeds expected size, ignoring")
		return
	}
	h.updateData = append(h.updateData, chunk...)
	h.updateMu.Unlock()
}

func (h *Handler) handleUpdateEnd() {
	h.updateMu.Lock()

	if len(h.updateData) == 0 {
		h.updateMu.Unlock()
		alog.Warn(alog.CatUpdate, "update_end: no data received")
		return
	}

	// 校验 MD5
	hash := md5.Sum(h.updateData)
	actualMD5 := hex.EncodeToString(hash[:])
	if h.updateMD5 != "" && actualMD5 != h.updateMD5 {
		alog.Error(alog.CatUpdate, "更新失败: MD5 不匹配", "expected", h.updateMD5, "actual", actualMD5)
		h.updateData = nil
		h.updateMu.Unlock()
		return
	}

	data := make([]byte, len(h.updateData))
	copy(data, h.updateData)
	h.updateData = nil
	h.updateMu.Unlock()

	alog.Info(alog.CatUpdate, "更新数据接收完成，准备替换", "md5", actualMD5, "size", len(data))

	// 获取当前可执行文件路径
	execPath, err := os.Executable()
	if err != nil {
		alog.Error(alog.CatUpdate, "更新失败: 获取路径", "error", err)
		return
	}

	// 写入临时文件
	tmpPath := execPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0755); err != nil {
		alog.Error(alog.CatUpdate, "更新失败: 写入临时文件", "error", err)
		return
	}

	// 跨平台安全重启：创建重启脚本
	if err := h.createRestartScript(execPath, tmpPath); err != nil {
		alog.Error(alog.CatUpdate, "更新失败: 创建重启脚本", "error", err)
		os.Remove(tmpPath)
		return
	}

	alog.Info(alog.CatUpdate, "更新成功，正在重启")
	alog.Flush()
	os.Exit(0)
}

// createRestartScript 创建跨平台重启脚本
func (h *Handler) createRestartScript(execPath, tmpPath string) error {
	execDir := filepath.Dir(execPath)

	var scriptPath, scriptContent string

	switch runtime.GOOS {
	case "windows":
		scriptPath = filepath.Join(execDir, "aether_restart.bat")
		scriptContent = fmt.Sprintf(`@echo off
timeout /t 2 /nobreak >nul
move /y "%s" "%s"
start "" "%s"
del "%%~f0"
`, filepath.Clean(tmpPath), filepath.Clean(execPath), filepath.Clean(execPath))
	default: // linux, darwin
		scriptPath = filepath.Join(execDir, "aether_restart.sh")
		scriptContent = fmt.Sprintf(`#!/bin/sh
sleep 2
mv %s %s
chmod +x %s
exec %s &
rm -- "$0"
`, shellQuote(tmpPath), shellQuote(execPath), shellQuote(execPath), shellQuote(execPath))
	}

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		return err
	}

	// 启动重启脚本
	switch runtime.GOOS {
	case "windows":
		go func() {
			cmd := exec.Command("cmd", "/c", "start", "/b", scriptPath)
			if err := cmd.Start(); err != nil {
				alog.Error(alog.CatUpdate, "启动重启脚本失败", "error", err)
				return
			}
		}()
	default:
		go func() {
			cmd := exec.Command("/bin/sh", scriptPath)
			if err := cmd.Start(); err != nil {
				alog.Error(alog.CatUpdate, "启动重启脚本失败", "error", err)
				return
			}
		}()
	}

	return nil
}

// shellQuote 对路径进行 shell 安全引用
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func (h *Handler) handleProxyClosed(data interface{}) {
	closed, err := unmarshalData[model.ProxyClosedData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "proxy_closed unmarshal error", "error", err)
		return
	}
	alog.Info(alog.CatProxy, "proxy closed by server, stopping tunnel", "key", closed.Key)
	h.stopTunnel(closed.Key)

	h.proxyMu.Lock()
	delete(h.proxyInfo, closed.Key)
	delete(h.tunnelContexts, closed.Key)
	h.proxyMu.Unlock()

	// 回复确认
	if h.sender != nil {
		ack := model.WSMessage{
			Type: "proxy_close_ack",
			Data: closed.Key,
		}
		h.sender.WriteJSON(&ack)
	}
}

// ProxyOutboundData 出站代理数据
type ProxyOutboundData struct {
	ServerHost string `json:"server_host"`
	TunnelPort int    `json:"tunnel_port"`
	Token      string `json:"token"`
	LocalPort  int    `json:"local_port"`
}

func (h *Handler) handleProxyOutbound(data interface{}) {
	cmd, err := unmarshalData[ProxyOutboundData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "proxy_outbound unmarshal error", "error", err)
		return
	}

	alog.Info(alog.CatProxy, "proxy_outbound received",
		"serverHost", cmd.ServerHost,
		"tunnelPort", cmd.TunnelPort,
		"localPort", cmd.LocalPort)

	// 连接 Server 的 tunnel 端口
	tunnelAddr := net.JoinHostPort(cmd.ServerHost, fmt.Sprintf("%d", cmd.TunnelPort))
	localAddr := net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", cmd.LocalPort))

	key := fmt.Sprintf("outbound-%d", cmd.LocalPort)
	tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
	h.registerTunnel(key, tunnelCancel)

	go func() {
		defer h.stopTunnel(key)
		h.connectOutboundTunnel(tunnelCtx, tunnelAddr, localAddr, cmd.Token)
	}()
}

// connectOutboundTunnel 连接出站隧道
func (h *Handler) connectOutboundTunnel(ctx context.Context, tunnelAddr, localAddr, token string) {
	tunnelConn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "outbound tunnel dial failed", "addr", tunnelAddr, "error", err)
		return
	}
	defer tunnelConn.Close()

	// 发送认证
	if err := proto.WriteTunnelAuth(tunnelConn, token); err != nil {
		alog.Error(alog.CatTunnel, "outbound tunnel auth write failed", "error", err)
		return
	}

	// 等待确认
	ack := make([]byte, 1)
	tunnelConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(tunnelConn, ack); err != nil || ack[0] != 0x01 {
		alog.Error(alog.CatTunnel, "outbound tunnel ack failed", "error", err)
		return
	}
	tunnelConn.SetReadDeadline(time.Time{})

	alog.Info(alog.CatTunnel, "outbound tunnel connected", "addr", tunnelAddr)

	// 连接本地 FileAPI
	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "outbound local dial failed", "addr", localAddr, "error", err)
		return
	}
	defer localConn.Close()

	alog.Info(alog.CatTunnel, "outbound local connected", "addr", localAddr)

	// context 取消时关闭连接
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tunnelConn.Close()
			localConn.Close()
		case <-done:
		}
	}()

	// 双向搬运数据
	alog.Info(alog.CatTunnel, "outbound piping", "tunnel", tunnelAddr, "local", localAddr)
	pipeTCP(tunnelConn, localConn)
	close(done)

	alog.Info(alog.CatTunnel, "outbound tunnel closed", "addr", tunnelAddr)
}

func (h *Handler) handleProxy(data interface{}) {
	cmd, err := unmarshalData[model.CommandData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "proxy message unmarshal error", "error", err)
		return
	}

	if cmd.LocalIP == "" {
		cmd.LocalIP = "127.0.0.1"
	}

	alog.Info(alog.CatProxy, "proxy command",
		"serverHost", cmd.ServerHost, "remotePort", cmd.RemotePort,
		"localPort", cmd.LocalPort, "localIP", cmd.LocalIP,
		"protocol", cmd.Protocol, "tunnelPort", cmd.TunnelPort)

	key := fmt.Sprintf("%s-%d", h.cfg.ClientID, cmd.RemotePort)

	if cmd.Protocol == "websocket" {
		tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
		h.registerTunnel(key, tunnelCancel)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					alog.Error(alog.CatTunnel, "PANIC in WS tunnel", "error", r)
				}
				h.stopTunnel(key)
			}()
			h.runTunnelWS(tunnelCtx, cmd.ServerHost, cmd.Token, cmd.LocalPort, cmd.LocalIP, key)
		}()
		return
	}

	if cmd.Protocol == "udp" {
		tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
		h.registerTunnel(key, tunnelCancel)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					alog.Error(alog.CatTunnel, "PANIC in UDP tunnel", "error", r)
				}
				h.stopTunnel(key)
			}()
			h.runTunnelUDP(tunnelCtx, cmd.ServerHost, cmd.RemotePort, cmd.Token, cmd.LocalPort, cmd.LocalIP)
		}()
		return
	}

	tunnelCtx, tunnelCancel := context.WithCancel(h.baseCtx)
	h.registerTunnel(key, tunnelCancel)

	h.proxyMu.Lock()
	h.proxyInfo[key] = cmd
	h.tunnelContexts[key] = tunnelCtx
	h.proxyMu.Unlock()
}

func (h *Handler) handleTunnelRequest(data interface{}) {
	req, err := unmarshalData[model.TunnelRequestData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "tunnel_request unmarshal error", "error", err)
		return
	}

	h.proxyMu.RLock()
	info := h.proxyInfo[req.Key]
	ctx := h.tunnelContexts[req.Key]
	h.proxyMu.RUnlock()

	if info == nil {
		alog.Warn(alog.CatProxy, "tunnel_request: no proxy info for key", "key", req.Key)
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	localAddr := net.JoinHostPort(info.LocalIP, fmt.Sprintf("%d", info.LocalPort))
	tunnelAddr := net.JoinHostPort(info.ServerHost, fmt.Sprintf("%d", info.TunnelPort))

	alog.Info(alog.CatProxy, "tunnel_request: connecting", "key", req.Key, "local", localAddr, "tunnel", tunnelAddr)

	go h.connectAndPipe(ctx, tunnelAddr, localAddr, req.Token)
}

func (h *Handler) connectAndPipe(ctx context.Context, tunnelAddr, localAddr, token string) {
	tunnelConn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "tunnel dial failed", "addr", tunnelAddr, "error", err)
		return
	}
	defer tunnelConn.Close()

	if err := proto.WriteTunnelAuth(tunnelConn, token); err != nil {
		alog.Error(alog.CatTunnel, "tunnel auth write failed", "error", err)
		return
	}

	ack := make([]byte, 1)
	tunnelConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(tunnelConn, ack); err != nil || ack[0] != 0x01 {
		alog.Error(alog.CatTunnel, "tunnel ack failed", "error", err)
		return
	}
	tunnelConn.SetReadDeadline(time.Time{})

	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "local dial failed", "addr", localAddr, "error", err)
		return
	}
	defer localConn.Close()

	// context 取消时关闭连接
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tunnelConn.Close()
			localConn.Close()
		case <-done:
		}
	}()

	alog.Info(alog.CatTunnel, "tunnel paired, piping", "local", localAddr)
	pipeTCP(tunnelConn, localConn)
	close(done)
}

func pipeTCP(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(a, b)
		// 半关闭：关闭写端，通知对端不再发送
		if tc, ok := a.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	go func() {
		defer wg.Done()
		io.Copy(b, a)
		if tc, ok := b.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
	a.Close()
	b.Close()
}

// runTunnelUDP 运行 UDP 隧道
// 通过 TCP 连接到服务器的 UDP 隧道端口，然后转发 UDP 数据
func (h *Handler) runTunnelUDP(ctx context.Context, serverHost string, remotePort int, token string, localPort int, localIP string) {
	alog.Info(alog.CatTunnel, "启动 UDP 隧道",
		"remote", fmt.Sprintf("%s:%d", serverHost, remotePort),
		"local", fmt.Sprintf("%s:%d", localIP, localPort))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tunnelAddr := net.JoinHostPort(serverHost, fmt.Sprintf("%d", remotePort))
		alog.Info(alog.CatTunnel, "连接 UDP 隧道", "addr", tunnelAddr)

		conn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
		if err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道连接失败", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		alog.Info(alog.CatTunnel, "已连接到 UDP 隧道",
			"local", conn.LocalAddr(), "remote", conn.RemoteAddr())

		// 发送认证标记 "TUNL"
		if _, err := conn.Write([]byte("TUNL")); err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道认证标记发送失败", "error", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 发送 token 长度和 token
		tokenBytes := []byte(token)
		tokenLen := uint16(len(tokenBytes))
		if err := binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道 token 长度发送失败", "error", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		if _, err := conn.Write(tokenBytes); err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道 token 发送失败", "error", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		// 等待确认
		ack := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.ReadFull(conn, ack); err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道确认读取失败", "error", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		conn.SetReadDeadline(time.Time{})

		if ack[0] != 0x01 {
			alog.Error(alog.CatTunnel, "UDP 隧道确认无效", "byte", fmt.Sprintf("0x%02x", ack[0]))
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		alog.Info(alog.CatTunnel, "UDP 隧道认证成功")

		// 启动 UDP 数据转发
		h.handleUDPTunnel(conn, localIP, localPort)

		alog.Info(alog.CatTunnel, "UDP 隧道断开，正在重连")
		time.Sleep(1 * time.Second)
	}
}

// handleUDPTunnel 处理 UDP 隧道数据转发
func (h *Handler) handleUDPTunnel(conn net.Conn, localIP string, localPort int) {
	defer conn.Close()

	// 连接到本地 UDP 服务
	localAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort)))
	if err != nil {
		alog.Error(alog.CatTunnel, "本地 UDP 地址解析失败", "error", err)
		return
	}

	localConn, err := net.DialUDP("udp", nil, localAddr)
	if err != nil {
		alog.Error(alog.CatTunnel, "本地 UDP 连接失败", "error", err)
		return
	}
	defer localConn.Close()

	// 从服务器读取 UDP 数据并转发到本地
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer localConn.Close()
		for {
			// 读取 UDP 数据头：[2字节目标端口][2字节数据长度]
			var destPort uint16
			var dataLen uint16

			if err := binary.Read(conn, binary.BigEndian, &destPort); err != nil {
				if err != io.EOF {
					alog.Error(alog.CatTunnel, "UDP 隧道读取端口错误", "error", err)
				}
				return
			}

			if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
				alog.Error(alog.CatTunnel, "UDP 隧道读取长度错误", "error", err)
				return
			}

			if dataLen > 65535 {
				alog.Error(alog.CatTunnel, "UDP 数据包过大", "size", dataLen)
				return
			}

			data := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, data); err != nil {
				alog.Error(alog.CatTunnel, "UDP 隧道读取数据错误", "error", err)
				return
			}

			// 发送到本地 UDP 服务
			if _, err := localConn.Write(data); err != nil {
				alog.Error(alog.CatTunnel, "本地 UDP 发送错误", "error", err)
				continue
			}
		}
	}()

	// 从本地 UDP 服务读取响应并发送回服务器
	buf := make([]byte, 65535)
	for {
		n, err := localConn.Read(buf)
		if err != nil {
			alog.Error(alog.CatTunnel, "本地 UDP 读取错误", "error", err)
			break
		}

		if n == 0 {
			continue
		}

		// 获取本地端口（作为源端口）
		localPort := localConn.LocalAddr().(*net.UDPAddr).Port

		// 发送响应到服务器
		// 格式：[2字节源端口][2字节数据长度][数据]
		packet := make([]byte, 4+n)
		binary.BigEndian.PutUint16(packet[0:2], uint16(localPort))
		binary.BigEndian.PutUint16(packet[2:4], uint16(n))
		copy(packet[4:], buf[:n])

		if _, err := conn.Write(packet); err != nil {
			alog.Error(alog.CatTunnel, "UDP 隧道写入错误", "error", err)
			break
		}
	}

	wg.Wait()
}

func (h *Handler) runTunnelWS(ctx context.Context, serverHost string, token string, localPort int, localIP string, key string) {
	localTarget := net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		scheme := "wss"
		if h.cfg.UseHTTP {
			scheme = "ws"
		}
		tunnelURL := fmt.Sprintf("%s://%s:9909/tunnel", scheme, serverHost)
		alog.Info(alog.CatTunnel, "Starting WebSocket tunnel", "url", tunnelURL, "local", localTarget)

		conn, err := h.connectTunnelWS(tunnelURL, token)
		if err != nil {
			alog.Error(alog.CatTunnel, "WebSocket tunnel connect failed, retrying", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		mx := mux.New(conn)
		mx.LocalTarget = localTarget
		mx.OnNewChannel = mx.HandleChannel
		alog.Info(alog.CatTunnel, "WebSocket tunnel multiplexer created", "localTarget", localTarget)

		<-mx.Done()
		alog.Info(alog.CatTunnel, "WebSocket tunnel multiplexer closed, reconnecting")
		time.Sleep(1 * time.Second)
	}
}

func (h *Handler) connectTunnelWS(tunnelURL string, token string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !h.cfg.UseHTTP {
		dialer.TLSClientConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: h.cfg.Insecure,
		}
		sni := h.sniForURL(tunnelURL)
		if sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !h.cfg.UseHTTP {
		origin := h.originForURL(tunnelURL)
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(tunnelURL, header)
	if err != nil {
		return nil, fmt.Errorf("tunnel ws dial: %w", err)
	}

	authMsg := model.WSMessage{
		Type: "tunnel_auth",
		Data: model.TunnelAuthData{Token: token},
	}
	if err := ws.WriteJSON(authMsg); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel auth write: %w", err)
	}

	var resp model.TunnelReadyMsg
	if err := ws.ReadJSON(&resp); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel ready read: %w", err)
	}

	if resp.Type != "tunnel_ready" {
		ws.Close()
		return nil, fmt.Errorf("tunnel unexpected response type: %s", resp.Type)
	}

	alog.Info(alog.CatTunnel, "WebSocket tunnel authenticated and ready")
	return wsconn.New(ws), nil
}

func (h *Handler) sniForURL(rawURL string) string {
	if h.cfg.SNIOverride != "" {
		return h.cfg.SNIOverride
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

func (h *Handler) originForURL(rawURL string) string {
	if h.cfg.OriginOverride != "" {
		return h.cfg.OriginOverride
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" {
		return ""
	}
	if h.cfg.UseHTTP {
		return "http://" + host
	}
	return "https://" + host
}

func unmarshalData[T any](data interface{}) (*T, error) {
	switch v := data.(type) {
	case string:
		var result T
		if err := json.Unmarshal([]byte(v), &result); err != nil {
			return nil, fmt.Errorf("unmarshal string data: %w", err)
		}
		return &result, nil
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal data: %w", err)
		}
		var result T
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
		return &result, nil
	}
}

func (h *Handler) handlePing(data interface{}) {
	if h.sender == nil {
		return
	}
	h.sender.WriteJSON(&model.WSMessage{
		Type: "pong",
		Data: data,
	})
}
