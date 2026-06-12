package transport

import (
	"BBgrid/common/model"
	"BBgrid/common/wsconn"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSTransport WebSocket 传输层实现
type WSTransport struct {
	mu          sync.RWMutex
	conn        *websocket.Conn
	url         string
	useHTTP     bool
	insecure    bool
	sniOverride string
	origin      string
	timeout     time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
	connected   bool
	stopCh      chan struct{}
	onMessage   func(msg *model.WSMessage)
}

// WSOpt WebSocket 传输层选项
type WSOpt func(*WSTransport)

// WithHTTP 使用 HTTP 协议
func WithHTTP(useHTTP bool) WSOpt {
	return func(t *WSTransport) {
		t.useHTTP = useHTTP
	}
}

// WithInsecure 跳过 TLS 验证
func WithInsecure(insecure bool) WSOpt {
	return func(t *WSTransport) {
		t.insecure = insecure
	}
}

// WithSNIOverride 覆盖 SNI
func WithSNIOverride(sni string) WSOpt {
	return func(t *WSTransport) {
		t.sniOverride = sni
	}
}

// WithOrigin 覆盖 Origin 头
func WithOrigin(origin string) WSOpt {
	return func(t *WSTransport) {
		t.origin = origin
	}
}

// WithTimeout 设置连接超时
func WithTimeout(timeout time.Duration) WSOpt {
	return func(t *WSTransport) {
		t.timeout = timeout
	}
}

// WithOnMessage 设置消息回调
func WithOnMessage(handler func(msg *model.WSMessage)) WSOpt {
	return func(t *WSTransport) {
		t.onMessage = handler
	}
}

// NewWSTransport 创建 WebSocket 传输层
func NewWSTransport(url string, opts ...WSOpt) *WSTransport {
	t := &WSTransport{
		url:           url,
		timeout:       10 * time.Second,
		readTimeout:   40 * time.Second,
		writeTimeout:  10 * time.Second,
		stopCh:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Connect 连接到服务器
func (t *WSTransport) Connect(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	dialer := &websocket.Dialer{
		HandshakeTimeout: t.timeout,
	}

	if !t.useHTTP {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: t.insecure,
		}
		// 设置 SNI：优先使用覆盖值，否则从 URL 提取
		sni := t.sniOverride
		if sni == "" {
			sni = extractHost(t.url)
		}
		if sni != "" {
			tlsConfig.ServerName = sni
		}
		dialer.TLSClientConfig = tlsConfig
	}

	header := http.Header{}
	if !t.useHTTP {
		origin := t.origin
		if origin == "" {
			origin = "https://" + extractHost(t.url)
		}
		header.Set("Origin", origin)
	}

	conn, _, err := dialer.DialContext(ctx, t.url, header)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}

	t.conn = conn
	t.connected = true
	return nil
}

// Send 发送消息
func (t *WSTransport) Send(msg any) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.connected || t.conn == nil {
		return fmt.Errorf("not connected")
	}

	t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout))
	return t.conn.WriteJSON(msg)
}

// Receive 接收消息
func (t *WSTransport) Receive() (any, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if !t.connected || t.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	t.conn.SetReadDeadline(time.Now().Add(t.readTimeout))
	_, data, err := t.conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	var msg model.WSMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}

// Close 关闭连接
func (t *WSTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	select {
	case <-t.stopCh:
		// already closed
	default:
		close(t.stopCh)
	}

	if t.conn != nil {
		t.connected = false
		return t.conn.Close()
	}
	return nil
}

// IsConnected 是否已连接
func (t *WSTransport) IsConnected() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.connected
}

// RemoteAddr 远程地址
func (t *WSTransport) RemoteAddr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.conn != nil {
		return t.conn.RemoteAddr().String()
	}
	return ""
}

// SetTimeout 设置超时时间
func (t *WSTransport) SetTimeout(timeout time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timeout = timeout
}

// SetReadDeadline 设置读取截止时间
func (t *WSTransport) SetReadDeadline(deadline time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readTimeout = deadline
}

// SetWriteDeadline 设置写入截止时间
func (t *WSTransport) SetWriteDeadline(deadline time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writeTimeout = deadline
}

// GetConn 获取底层连接
func (t *WSTransport) GetConn() net.Conn {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.conn != nil {
		return wsconn.New(t.conn)
	}
	return nil
}

// SetConn 设置底层连接
func (t *WSTransport) SetConn(conn net.Conn) {
	// WebSocket 传输层不支持直接设置连接
}

// StartMessageLoop 启动消息循环
func (t *WSTransport) StartMessageLoop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.stopCh:
				return
			default:
			}

			msg, err := t.Receive()
			if err != nil {
				if t.connected {
					t.mu.Lock()
					t.connected = false
					t.mu.Unlock()
				}
				return
			}

			if wsMsg, ok := msg.(*model.WSMessage); ok && t.onMessage != nil {
				t.onMessage(wsMsg)
			}
		}
	}()
}

// extractHost 从 URL 中提取主机名
func extractHost(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
