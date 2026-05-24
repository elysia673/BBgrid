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

// UDPTunnelConfig UDP隧道配置（KCP参数）
type UDPTunnelConfig struct {
	// KCP核心参数
	NoDelay  int // 是否启用nodelay模式, 0-不启用, 1-启用
	Interval int // 内部刷新时钟, 单位ms, 范围10-100
	Resend   int // 快速重传触发次数, 范围0-10
	NC       int // 是否关闭流控, 0-开启, 1-关闭

	// 加密配置
	Crypt string // 加密方式
	Key   string // 加密密钥
}

// DefaultUDPTunnelConfig 默认UDP隧道配置
func DefaultUDPTunnelConfig(key string) *UDPTunnelConfig {
	return &UDPTunnelConfig{
		NoDelay:  1,
		Interval: 10,
		Resend:   2,
		NC:       1,
		Crypt:    "aes",
		Key:      key,
	}
}

// connectUDPTunnel 连接UDP隧道
func (h *Handler) connectUDPTunnel(tunnelAddr, localAddr, token string, config *UDPTunnelConfig) {
	if config == nil {
		config = DefaultUDPTunnelConfig(h.cfg.UDPTunnelKey)
	}

	// 创建KCP连接
	block, err := kcp.NewAESBlockCrypt([]byte(config.Key))
	if err != nil {
		alog.Error(alog.CatTunnel, "udp create crypt failed", "error", err)
		return
	}

	// 连接KCP服务器
	sess, err := kcp.DialWithOptions(tunnelAddr, block, 0, 0)
	if err != nil {
		alog.Error(alog.CatTunnel, "udp dial failed", "addr", tunnelAddr, "error", err)
		return
	}
	defer sess.Close()

	// 设置KCP参数
	sess.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NC)

	// 设置读取超时
	sess.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 发送认证
	if err := proto.WriteTunnelAuth(sess, token); err != nil {
		alog.Error(alog.CatTunnel, "udp tunnel auth write failed", "error", err)
		return
	}

	// 等待ACK
	ack := make([]byte, 1)
	if _, err := io.ReadFull(sess, ack); err != nil || ack[0] != 0x01 {
		alog.Error(alog.CatTunnel, "udp tunnel ack failed", "error", err)
		return
	}
	sess.SetReadDeadline(time.Time{})

	// 连接本地服务
	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		alog.Error(alog.CatTunnel, "local dial failed", "addr", localAddr, "error", err)
		return
	}
	defer localConn.Close()

	alog.Info(alog.CatTunnel, "udp tunnel paired, piping", "local", localAddr)

	// 双向转发
	pipeUDP(sess, localConn)
}

// pipeUDP UDP与TCP之间的双向转发
func pipeUDP(udpConn *kcp.UDPSession, tcpConn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer udpConn.Close()
		io.Copy(udpConn, tcpConn)
	}()

	go func() {
		defer wg.Done()
		defer tcpConn.Close()
		io.Copy(tcpConn, udpConn)
	}()

	wg.Wait()
}

// handleUDPTunnelRequest 处理UDP隧道请求
func (h *Handler) handleUDPTunnelRequest(data interface{}) {
	req, err := unmarshalData[model.TunnelRequestData](data)
	if err != nil {
		alog.Error(alog.CatProxy, "udp tunnel_request unmarshal error", "error", err)
		return
	}

	h.proxyMu.RLock()
	info := h.proxyInfo[req.Key]
	h.proxyMu.RUnlock()

	if info == nil {
		alog.Warn(alog.CatProxy, "udp tunnel_request: no proxy info for key", "key", req.Key)
		return
	}

	localAddr := net.JoinHostPort(info.LocalIP, fmt.Sprintf("%d", info.LocalPort))
	tunnelAddr := net.JoinHostPort(info.ServerHost, fmt.Sprintf("%d", info.TunnelPort))

	alog.Info(alog.CatProxy, "udp tunnel_request: connecting", "key", req.Key, "local", localAddr, "tunnel", tunnelAddr)

	// 使用默认UDP配置
	config := DefaultUDPTunnelConfig(h.cfg.UDPTunnelKey)

	go h.connectUDPTunnel(tunnelAddr, localAddr, req.Token, config)
}
