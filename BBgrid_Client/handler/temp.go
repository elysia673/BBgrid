package handler

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/proto"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// TempMessageSender 消息发送器接口
type TempMessageSender interface {
	WriteJSON(v interface{}) error
}

// TempHandler 临时节点消息处理器
type TempHandler struct {
	sender       TempMessageSender
	proxyMu      sync.RWMutex
	proxyInfo    map[string]*model.CommandData
	udpTunnelKey string
}

// NewTempHandler 创建临时节点处理器
func NewTempHandler() *TempHandler {
	return &TempHandler{
		proxyInfo: make(map[string]*model.CommandData),
	}
}

// SetUDPTunnelKey 设置 UDP 隧道加密密钥
func (h *TempHandler) SetUDPTunnelKey(key string) {
	h.udpTunnelKey = key
}

// SetSender 设置消息发送器
func (h *TempHandler) SetSender(sender TempMessageSender) {
	h.sender = sender
}

// Handle 处理消息
func (h *TempHandler) Handle(msg *model.WSMessage) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatSystem, "PANIC in temp handler", "error", r)
		}
	}()

	switch msg.Type {
	case "proxy":
		h.handleProxy(msg.Data)
	case "proxy_closed":
		h.handleProxyClosed(msg.Data)
	case "tunnel_request":
		h.handleTunnelRequest(msg.Data)
	case "ping":
		h.handlePing(msg.Data)
	}
}

func (h *TempHandler) handleProxy(data interface{}) {
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

	key := fmt.Sprintf("temp-%d", cmd.RemotePort)

	h.proxyMu.Lock()
	h.proxyInfo[key] = cmd
	h.proxyMu.Unlock()

	alog.Info(alog.CatProxy, "proxy mapping received", "serverHost", cmd.ServerHost, "remotePort", cmd.RemotePort, "localIP", cmd.LocalIP, "localPort", cmd.LocalPort, "protocol", cmd.Protocol)
}

func (h *TempHandler) handleProxyClosed(data interface{}) {
	closed, err := unmarshalData[model.ProxyClosedData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "proxy_closed unmarshal error", "error", err)
		return
	}
	alog.Info(alog.CatProxy, "proxy closed", "key", closed.Key)

	h.proxyMu.Lock()
	delete(h.proxyInfo, closed.Key)
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

func (h *TempHandler) handleTunnelRequest(data interface{}) {
	req, err := unmarshalData[model.TunnelRequestData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "tunnel_request unmarshal error", "error", err)
		return
	}

	h.proxyMu.RLock()
	info := h.proxyInfo[req.Key]
	h.proxyMu.RUnlock()

	if info == nil {
		alog.Warn(alog.CatProxy, "tunnel_request: no proxy info for key", "key", req.Key)
		return
	}

	localAddr := net.JoinHostPort(info.LocalIP, fmt.Sprintf("%d", info.LocalPort))
	tunnelAddr := net.JoinHostPort(info.ServerHost, fmt.Sprintf("%d", info.TunnelPort))

	alog.Info(alog.CatProxy, "tunnel_request: connecting", "key", req.Key, "local", localAddr, "tunnel", tunnelAddr, "protocol", info.Protocol)

	// 根据协议选择隧道类型
	if info.Protocol == "udp" {
		config := DefaultUDPTunnelConfig(h.udpTunnelKey)
		go h.connectUDPTunnel(tunnelAddr, localAddr, req.Token, config)
	} else {
		go h.connectAndPipe(tunnelAddr, localAddr, req.Token)
	}
}

func (h *TempHandler) handlePing(data interface{}) {
	if h.sender == nil {
		return
	}
	h.sender.WriteJSON(&model.WSMessage{
		Type: "pong",
		Data: data,
	})
}

func (h *TempHandler) connectAndPipe(tunnelAddr, localAddr, token string) {
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

	alog.Info(alog.CatTunnel, "tunnel paired, piping", "local", localAddr)
	pipeTCP(tunnelConn, localConn)
}

func (h *TempHandler) connectUDPTunnel(tunnelAddr, localAddr, token string, config *UDPTunnelConfig) {
	if config == nil {
		config = DefaultUDPTunnelConfig(h.udpTunnelKey)
	}

	block, err := kcp.NewAESBlockCrypt([]byte(config.Key))
	if err != nil {
		alog.Error(alog.CatTunnel, "udp create crypt failed", "error", err)
		return
	}

	sess, err := kcp.DialWithOptions(tunnelAddr, block, 0, 0)
	if err != nil {
		alog.Error(alog.CatTunnel, "udp dial failed", "addr", tunnelAddr, "error", err)
		return
	}
	defer sess.Close()

	sess.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NC)
	sess.SetReadDeadline(time.Now().Add(10 * time.Second))

	if err := proto.WriteTunnelAuth(sess, token); err != nil {
		alog.Error(alog.CatTunnel, "udp tunnel auth write failed", "error", err)
		return
	}

	ack := make([]byte, 1)
	if _, err := io.ReadFull(sess, ack); err != nil || ack[0] != 0x01 {
		alog.Error(alog.CatTunnel, "udp tunnel ack failed", "error", err)
		return
	}
	sess.SetReadDeadline(time.Time{})

	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "local dial failed", "addr", localAddr, "error", err)
		return
	}
	defer localConn.Close()

	alog.Info(alog.CatTunnel, "udp tunnel paired, piping", "local", localAddr)
	pipeUDP(sess, localConn)
}
