// Package tunnel 提供隧道管理抽象。
package tunnel

import (
	"context"
	"io"
	"net"
	"sync"
)

// TunnelType 隧道类型
type TunnelType string

const (
	TunnelTypeTCP TunnelType = "tcp"
	TunnelTypeUDP TunnelType = "udp"
	TunnelTypeWS  TunnelType = "ws"
)

// TunnelState 隧道状态
type TunnelState string

const (
	StateIdle       TunnelState = "idle"
	StateConnecting TunnelState = "connecting"
	StateConnected  TunnelState = "connected"
	StateClosed     TunnelState = "closed"
)

// Config 隧道配置
type Config struct {
	Type       TunnelType `json:"type"`
	Key        string     `json:"key"`
	ServerHost string     `json:"server_host"`
	TunnelPort int        `json:"tunnel_port"`
	LocalIP    string     `json:"local_ip"`
	LocalPort  int        `json:"local_port"`
	RemotePort int        `json:"remote_port"`
	Token      string     `json:"token"`
	UseHTTP    bool       `json:"use_http"`
	Insecure   bool       `json:"insecure"`
	SNI        string     `json:"sni"`
	Origin     string     `json:"origin"`
}

// Tunnel 隧道接口
type Tunnel interface {
	// Start 启动隧道
	Start(ctx context.Context) error

	// Stop 停止隧道
	Stop() error

	// GetState 获取隧道状态
	GetState() TunnelState

	// GetKey 获取隧道 Key
	GetKey() string

	// GetType 获取隧道类型
	GetType() TunnelType

	// SetLocalTarget 设置本地目标地址
	SetLocalTarget(ip string, port int)
}

// Manager 隧道管理器
type Manager struct {
	mu      sync.RWMutex
	tunnels map[string]Tunnel
	stopCh  chan struct{}
}

// NewManager 创建隧道管理器
func NewManager() *Manager {
	return &Manager{
		tunnels: make(map[string]Tunnel),
		stopCh:  make(chan struct{}),
	}
}

// Create 创建隧道
func (m *Manager) Create(config Config) (Tunnel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var tunnel Tunnel
	switch config.Type {
	case TunnelTypeTCP:
		tunnel = newTCPTunnel(config)
	case TunnelTypeUDP:
		tunnel = newUDPTunnel(config)
	case TunnelTypeWS:
		tunnel = newWSTunnel(config)
	default:
		return nil, ErrUnsupportedTunnelType
	}

	m.tunnels[config.Key] = tunnel
	return tunnel, nil
}

// Get 获取隧道
func (m *Manager) Get(key string) (Tunnel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tunnels[key]
	return t, ok
}

// Delete 删除隧道
func (m *Manager) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tunnels[key]
	if !ok {
		return nil
	}

	delete(m.tunnels, key)
	return t.Stop()
}

// Stop 停止所有隧道
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.stopCh:
	default:
		close(m.stopCh)
	}

	for _, t := range m.tunnels {
		t.Stop()
	}
	m.tunnels = make(map[string]Tunnel)
}

// List 列出所有隧道
func (m *Manager) List() []Tunnel {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tunnels := make([]Tunnel, 0, len(m.tunnels))
	for _, t := range m.tunnels {
		tunnels = append(tunnels, t)
	}
	return tunnels
}

// baseTunnel 隧道基础实现
type baseTunnel struct {
	mu          sync.RWMutex
	config      Config
	state       TunnelState
	localIP     string
	localPort   int
	cancel      context.CancelFunc
}

func (t *baseTunnel) GetKey() string {
	return t.config.Key
}

func (t *baseTunnel) GetType() TunnelType {
	return t.config.Type
}

func (t *baseTunnel) GetState() TunnelState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

func (t *baseTunnel) SetLocalTarget(ip string, port int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.localIP = ip
	t.localPort = port
}

func (t *baseTunnel) setState(state TunnelState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = state
}

// pipeTCP 双向转发 TCP 连接
func pipeTCP(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(a, b)
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

// 错误定义
var (
	ErrUnsupportedTunnelType = &TunnelError{"unsupported tunnel type"}
)

type TunnelError struct {
	msg string
}

func (e *TunnelError) Error() string {
	return e.msg
}
