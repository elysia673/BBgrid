package tunnel

import (
	"BBgrid/common/mux"
	"BBgrid/common/wsconn"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// wsTunnel WebSocket 隧道实现
type wsTunnel struct {
	baseTunnel
}

// newWSTunnel 创建 WebSocket 隧道
func newWSTunnel(config Config) *wsTunnel {
	return &wsTunnel{
		baseTunnel: baseTunnel{
			config:    config,
			state:     StateIdle,
			localIP:   config.LocalIP,
			localPort: config.LocalPort,
		},
	}
}

// Start 启动 WebSocket 隧道
func (t *wsTunnel) Start(ctx context.Context) error {
	ctx, t.cancel = context.WithCancel(ctx)
	t.setState(StateConnecting)

	go t.run(ctx)
	return nil
}

// Stop 停止 WebSocket 隧道
func (t *wsTunnel) Stop() error {
	t.setState(StateClosed)
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

// run 运行 WebSocket 隧道
func (t *wsTunnel) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.connectAndMux(ctx); err != nil {
			t.setState(StateConnecting)
			time.Sleep(1 * time.Second)
			continue
		}

		t.setState(StateClosed)
		return
	}
}

// connectAndMux 连接并建立多路复用
func (t *wsTunnel) connectAndMux(ctx context.Context) error {
	scheme := "wss"
	if t.config.UseHTTP {
		scheme = "ws"
	}

	tunnelURL := fmt.Sprintf("%s://%s:9909/tunnel", scheme, t.config.ServerHost)

	// 连接 WebSocket
	wsConn, err := t.connectWS(tunnelURL)
	if err != nil {
		return fmt.Errorf("ws connect: %w", err)
	}
	defer wsConn.Close()

	// 创建多路复用器
	localTarget := net.JoinHostPort(t.localIP, fmt.Sprintf("%d", t.localPort))
	mx := mux.New(wsconn.New(wsConn))
	mx.LocalTarget = localTarget
	mx.OnNewChannel = mx.HandleChannel

	t.setState(StateConnected)

	// 等待关闭
	select {
	case <-ctx.Done():
	case <-mx.Done():
	}

	return nil
}

// connectWS 连接 WebSocket
func (t *wsTunnel) connectWS(tunnelURL string) (*websocket.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !t.config.UseHTTP {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: t.config.Insecure,
		}
		if t.config.SNI != "" {
			tlsConfig.ServerName = t.config.SNI
		} else if host := extractHost(tunnelURL); host != "" {
			tlsConfig.ServerName = host
		}
		dialer.TLSClientConfig = tlsConfig
	}

	header := http.Header{}
	if !t.config.UseHTTP {
		origin := t.config.Origin
		if origin == "" {
			origin = "https://" + extractHost(tunnelURL)
		}
		header.Set("Origin", origin)
	}

	ws, _, err := dialer.Dial(tunnelURL, header)
	if err != nil {
		return nil, err
	}

	// 发送认证
	authMsg := map[string]any{
		"type": "tunnel_auth",
		"data": map[string]string{
			"token": t.config.Token,
		},
	}
	if err := ws.WriteJSON(authMsg); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel auth: %w", err)
	}

	// 等待响应
	var resp map[string]any
	if err := ws.ReadJSON(&resp); err != nil {
		ws.Close()
		return nil, fmt.Errorf("tunnel ready: %w", err)
	}

	if respType, ok := resp["type"].(string); !ok || respType != "tunnel_ready" {
		ws.Close()
		return nil, fmt.Errorf("unexpected response: %v", resp)
	}

	return ws, nil
}

// extractHost 从 URL 中提取主机名
func extractHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
