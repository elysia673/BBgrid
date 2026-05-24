package manager

import (
	alog "BBgrid/common/log"
	"BBgrid/common/mux"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ProxyInfo 代理信息。
type ProxyInfo struct {
	RemotePort int
	LocalPort  int
	LocalIP    string
	Protocol   string
	BindAddr   string
	Listener   io.Closer
}

// ClientTable 存储单个客户端的状态信息。
type ClientTable struct {
	clientID    string
	conn        *Connection
	remoteAddr  string
	connectedAt int64
	host        string

	proxies map[int]*ProxyInfo
	proxyMu sync.RWMutex

	multiplexers map[string]*mux.Multiplexer
	wsTokens     map[string]string
	tunnelTokens map[string]string // token -> key (用于隧道端口认证)
	tunnelMu     sync.RWMutex

	udpTunnels map[string]net.Conn
	udpMu      sync.RWMutex
}

// NewClientTable 创建新的客户端表。
func NewClientTable(clientID string, conn *Connection, remoteAddr string, connectedAt int64, host string) *ClientTable {
	return &ClientTable{
		clientID:     clientID,
		conn:         conn,
		remoteAddr:   remoteAddr,
		connectedAt:  connectedAt,
		host:         host,
		proxies:      make(map[int]*ProxyInfo),
		multiplexers: make(map[string]*mux.Multiplexer),
		wsTokens:     make(map[string]string),
		tunnelTokens: make(map[string]string),
		udpTunnels:   make(map[string]net.Conn),
	}
}

func (t *ClientTable) ClientID() string   { return t.clientID }
func (t *ClientTable) Conn() *Connection  { return t.conn }
func (t *ClientTable) RemoteAddr() string { return t.remoteAddr }
func (t *ClientTable) ConnectedAt() int64 { return t.connectedAt }
func (t *ClientTable) Host() string       { return t.host }
func (t *ClientTable) SetHost(h string)   { t.host = h }

func (t *ClientTable) TunnelHost(publicIP string) string {
	if t.host == "" {
		return publicIP
	}
	return t.host
}

func (t *ClientTable) TunnelKey(port int) string {
	return fmt.Sprintf("%s-%d", t.clientID, port)
}

// GetProxyPort 从 key 中提取端口号
func (t *ClientTable) GetProxyPort(key string) int {
	for port := range t.proxies {
		if t.TunnelKey(port) == key {
			return port
		}
	}
	return 0
}

// 代理操作

func (t *ClientTable) AddProxy(p *ProxyInfo) {
	t.proxyMu.Lock()
	t.proxies[p.RemotePort] = p
	t.proxyMu.Unlock()
}

func (t *ClientTable) RemoveProxy(port int) {
	t.proxyMu.Lock()
	delete(t.proxies, port)
	t.proxyMu.Unlock()
}

func (t *ClientTable) GetProxy(port int) *ProxyInfo {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	return t.proxies[port]
}

func (t *ClientTable) GetProxyListener(port int) io.Closer {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	if p, ok := t.proxies[port]; ok {
		return p.Listener
	}
	return nil
}

func (t *ClientTable) ListProxies() []*ProxyInfo {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	list := make([]*ProxyInfo, 0, len(t.proxies))
	for _, p := range t.proxies {
		list = append(list, p)
	}
	return list
}

func (t *ClientTable) ProxyCount() int {
	t.proxyMu.RLock()
	defer t.proxyMu.RUnlock()
	return len(t.proxies)
}

// TCP 隧道操作

func (t *ClientTable) PutMultiplexer(key string, mx *mux.Multiplexer) {
	t.tunnelMu.Lock()
	old := t.multiplexers[key]
	t.multiplexers[key] = mx
	t.tunnelMu.Unlock()
	if old != nil {
		old.Close()
	}
}

func (t *ClientTable) GetMultiplexer(key string) (*mux.Multiplexer, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	m, ok := t.multiplexers[key]
	if !ok {
		return nil, fmt.Errorf("no multiplexer for key %s", key)
	}
	return m, nil
}

func (t *ClientTable) RemoveTunnel(key string) {
	t0 := time.Now()
	t.tunnelMu.Lock()
	alog.Info(alog.CatMux, "removeTunnel lock acquired", "key", key, "elapsed", time.Since(t0))
	m := t.multiplexers[key]
	if m != nil {
		delete(t.multiplexers, key)
	}
	t.tunnelMu.Unlock()
	alog.Info(alog.CatMux, "removeTunnel unlock done", "key", key, "total", time.Since(t0))

	if m != nil {
		go m.Close()
	}
}

func (t *ClientTable) RemoveMux(key string, mx *mux.Multiplexer) {
	t.tunnelMu.Lock()
	if t.multiplexers[key] == mx {
		delete(t.multiplexers, key)
	}
	t.tunnelMu.Unlock()
}

// WS 令牌操作

func (t *ClientTable) StoreWSToken(token, key string) {
	t.tunnelMu.Lock()
	t.wsTokens[token] = key
	t.tunnelMu.Unlock()
}

func (t *ClientTable) GetWSToken(token string) (string, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	key, ok := t.wsTokens[token]
	if !ok {
		return "", fmt.Errorf("invalid token")
	}
	return key, nil
}

func (t *ClientTable) RemoveWSToken(token string) {
	t.tunnelMu.Lock()
	delete(t.wsTokens, token)
	t.tunnelMu.Unlock()
}

// 隧道令牌操作（用于隧道端口认证）

func (t *ClientTable) StoreTunnelToken(token, key string) {
	t.tunnelMu.Lock()
	t.tunnelTokens[token] = key
	t.tunnelMu.Unlock()
}

// RemoveTunnelTokenByKey 根据代理 key 删除隧道 token
func (t *ClientTable) RemoveTunnelTokenByKey(key string) {
	t.tunnelMu.Lock()
	for token, k := range t.tunnelTokens {
		if k == key {
			delete(t.tunnelTokens, token)
		}
	}
	t.tunnelMu.Unlock()
}

// UDP 隧道操作

func (t *ClientTable) SetUDPTunnel(key string, conn net.Conn) {
	t.udpMu.Lock()
	if old := t.udpTunnels[key]; old != nil {
		old.Close()
	}
	t.udpTunnels[key] = conn
	t.udpMu.Unlock()
}

func (t *ClientTable) GetUDPTunnel(key string) net.Conn {
	t.udpMu.RLock()
	defer t.udpMu.RUnlock()
	return t.udpTunnels[key]
}

// Cleanup 关闭此客户端关联的所有代理、隧道和连接。

func (t *ClientTable) Cleanup() {
	t.proxyMu.Lock()
	for _, p := range t.proxies {
		if p.Listener != nil {
			p.Listener.Close()
		}
	}
	t.proxyMu.Unlock()

	t.tunnelMu.Lock()
	muxes := make([]*mux.Multiplexer, 0, len(t.multiplexers))
	for _, mx := range t.multiplexers {
		muxes = append(muxes, mx)
	}
	t.multiplexers = make(map[string]*mux.Multiplexer)
	t.tunnelMu.Unlock()

	for _, mx := range muxes {
		go mx.Close()
	}

	t.udpMu.Lock()
	for _, c := range t.udpTunnels {
		c.Close()
	}
	t.udpMu.Unlock()
}
