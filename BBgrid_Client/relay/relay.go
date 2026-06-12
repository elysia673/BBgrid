// Package relay 提供中继管理抽象。
package relay

import (
	"BBgrid/common/mux"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsConnAdapter WebSocket 连接适配器
type wsConnAdapter struct {
	ws *websocket.Conn
}

func (a *wsConnAdapter) Read(b []byte) (int, error) {
	_, reader, err := a.ws.NextReader()
	if err != nil {
		return 0, err
	}
	return reader.Read(b)
}

func (a *wsConnAdapter) Write(b []byte) (int, error) {
	writer, err := a.ws.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	defer writer.Close()
	return writer.Write(b)
}

func (a *wsConnAdapter) Close() error {
	return a.ws.Close()
}

func (a *wsConnAdapter) LocalAddr() net.Addr {
	return a.ws.LocalAddr()
}

func (a *wsConnAdapter) RemoteAddr() net.Addr {
	return a.ws.RemoteAddr()
}

func (a *wsConnAdapter) SetDeadline(t time.Time) error {
	if err := a.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return a.ws.SetWriteDeadline(t)
}

func (a *wsConnAdapter) SetReadDeadline(t time.Time) error {
	return a.ws.SetReadDeadline(t)
}

func (a *wsConnAdapter) SetWriteDeadline(t time.Time) error {
	return a.ws.SetWriteDeadline(t)
}

// RelayRole 中继角色
type RelayRole string

const (
	RoleSource RelayRole = "source"
	RoleTarget RelayRole = "target"
)

// RelayState 中继状态
type RelayState string

const (
	StateIdle       RelayState = "idle"
	StateConnecting RelayState = "connecting"
	StateConnected  RelayState = "connected"
	StateClosed     RelayState = "closed"
)

// Config 中继配置
type Config struct {
	SessionID    string     `json:"session_id"`
	Role         RelayRole  `json:"role"`
	Protocol     string     `json:"protocol"`
	ServerHost   string     `json:"server_host"`
	SourcePort   int        `json:"source_port"`
	TargetPort   int        `json:"target_port"`
	SourceLocalIP string   `json:"source_local_ip"`
	TargetLocalIP string   `json:"target_local_ip"`
	Token        string     `json:"token"`
	ClientID     string     `json:"client_id"`
	UseHTTP      bool       `json:"use_http"`
	Insecure     bool       `json:"insecure"`
	SNI          string     `json:"sni"`
	Origin       string     `json:"origin"`
}

// Relay 中继接口
type Relay interface {
	// Start 启动中继
	Start(ctx context.Context) error

	// Stop 停止中继
	Stop() error

	// GetState 获取中继状态
	GetState() RelayState

	// GetSessionID 获取会话 ID
	GetSessionID() string

	// GetRole 获取角色
	GetRole() RelayRole
}

// Manager 中继管理器
type Manager struct {
	mu      sync.RWMutex
	relays  map[string]Relay
	stopCh  chan struct{}
	sender  func(msg any) error
}

// NewManager 创建中继管理器
func NewManager() *Manager {
	return &Manager{
		relays: make(map[string]Relay),
		stopCh: make(chan struct{}),
	}
}

// SetSender 设置消息发送器
func (m *Manager) SetSender(sender func(msg any) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sender = sender
}

// Create 创建中继
func (m *Manager) Create(config Config) (Relay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var relay Relay
	switch config.Role {
	case RoleSource:
		relay = newSourceRelay(config, m.sender)
	case RoleTarget:
		relay = newTargetRelay(config, m.sender)
	default:
		return nil, ErrUnsupportedRole
	}

	m.relays[config.SessionID] = relay
	return relay, nil
}

// Get 获取中继
func (m *Manager) Get(sessionID string) (Relay, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.relays[sessionID]
	return r, ok
}

// Delete 删除中继
func (m *Manager) Delete(sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	r, ok := m.relays[sessionID]
	if !ok {
		return nil
	}

	delete(m.relays, sessionID)
	return r.Stop()
}

// Stop 停止所有中继
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}

	for _, r := range m.relays {
		r.Stop()
	}
	m.relays = make(map[string]Relay)
}

// List 列出所有中继
func (m *Manager) List() []Relay {
	m.mu.RLock()
	defer m.mu.RUnlock()

	relays := make([]Relay, 0, len(m.relays))
	for _, r := range m.relays {
		relays = append(relays, r)
	}
	return relays
}

// baseRelay 中继基础实现
type baseRelay struct {
	mu      sync.RWMutex
	config  Config
	state   RelayState
	cancel  context.CancelFunc
	sender  func(msg any) error
}

func (r *baseRelay) GetSessionID() string {
	return r.config.SessionID
}

func (r *baseRelay) GetRole() RelayRole {
	return r.config.Role
}

func (r *baseRelay) GetState() RelayState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *baseRelay) setState(state RelayState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *baseRelay) sendStatus(status string, msg string) {
	if r.sender == nil {
		return
	}
	r.sender(map[string]any{
		"type": "relay_established",
		"data": map[string]string{
			"session_id": r.config.SessionID,
			"status":     status,
			"message":    msg,
		},
	})
}

func (r *baseRelay) sendClosed() {
	if r.sender == nil {
		return
	}
	r.sender(map[string]any{
		"type": "relay_closed",
		"data": map[string]string{
			"session_id": r.config.SessionID,
			"status":     "closed",
		},
	})
}

// connectRelay 连接到中继服务器
func connectRelay(config Config) (*websocket.Conn, error) {
	scheme := "wss"
	if config.UseHTTP {
		scheme = "ws"
	}

	relayURL := fmt.Sprintf("%s://%s:9909/relay?session=%s&token=%s&role=%s&client_id=%s",
		scheme, config.ServerHost, config.SessionID, config.Token, config.Role, config.ClientID)

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	u, _ := url.Parse(relayURL)
	hostname := ""
	if u != nil {
		hostname = u.Hostname()
	}

	if !config.UseHTTP {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: config.Insecure,
		}
		if config.SNI != "" {
			tlsConfig.ServerName = config.SNI
		} else if hostname != "" {
			tlsConfig.ServerName = hostname
		}
		dialer.TLSClientConfig = tlsConfig
	}

	header := http.Header{}
	if !config.UseHTTP {
		origin := config.Origin
		if origin == "" && hostname != "" {
			origin = "https://" + hostname
		}
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(relayURL, header)
	if err != nil {
		return nil, fmt.Errorf("relay dial: %w", err)
	}

	return ws, nil
}

// bridgeChannel 桥接本地连接和多路复用通道
func bridgeChannel(localConn net.Conn, channel *mux.Channel, sessionID string) {
	defer func() {
		if r := recover(); r != nil {
			// log panic
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(channel, localConn)
	}()

	go func() {
		defer wg.Done()
		io.Copy(localConn, channel)
	}()

	wg.Wait()
	localConn.Close()
	channel.Mux.CloseChannel(channel.Port)
}

// 错误定义
var (
	ErrUnsupportedRole = &RelayError{"unsupported relay role"}
	ErrRelayClosed     = &RelayError{"relay closed"}
)

type RelayError struct {
	msg string
}

func (e *RelayError) Error() string {
	return e.msg
}
