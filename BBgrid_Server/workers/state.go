package workers

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"BBgrid/common/store"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ==================== 接口定义 ====================

// StateStore 状态存储接口
//
// 控制器通过此接口访问状态，不直接依赖具体实现。
// 未来可以用 gRPC 实现此接口。
type StateStore interface {
	// 客户端操作
	AddClient(clientID string, conn ClientConn, remoteAddr string)
	RemoveClient(clientID string)
	GetClient(clientID string) (ClientState, bool)
	ListClients() []ClientInfo
	SendCommand(clientID string, cmd any) error

	// 代理操作
	AddProxy(clientID string, proxy ProxyState)
	RemoveProxy(clientID string, port int)
	GetProxy(clientID string, port int) (ProxyState, bool)
	ListProxies() []ProxyInfo
	RegisterPort(clientID string, port int)
	UnregisterPort(port int)
	GetClientIDByPort(port int) (string, bool)

	// 隧道操作
	StoreTunnelToken(token, key string)
	RemoveTunnelTokenByKey(key string)
	FindTableByWSToken(token string) (ClientState, string, error)

	// 中继操作
	CreateRelaySession(session RelaySession)
	GetRelaySession(sessionID string) (RelaySession, bool)
	RemoveRelaySession(sessionID string)
	ListRelaySessions() []RelaySession

	// 公共信息
	GetPublicIP() string
}

// ClientConn 客户端连接接口
type ClientConn interface {
	WriteJSON(v any) error
	Close() error
	GetHost() string
	GetRemoteIP() string
	IsTemp() bool
	Latency() time.Duration
	LastPingAt() time.Time
}

// ClientState 客户端状态接口
type ClientState interface {
	ClientID() string
	Conn() ClientConn
	RemoteAddr() string
	ConnectedAt() int64
	Host() string
	SetHost(h string)
	TunnelHost(publicIP string) string
	TunnelKey(port int) string
	AddProxy(p ProxyState)
	RemoveProxy(port int)
	GetProxy(port int) (ProxyState, bool)
	GetProxyListener(port int) io.Closer
	ListProxies() []ProxyState
	ProxyCount() int
	StoreTunnelToken(token, key string)
	RemoveTunnelTokenByKey(key string)
	FindTableByWSToken(token string) (string, error)
	Cleanup()

	// 隧道连接操作
	PutTunnel(key string, tc *tunnelConn)
	GetTunnel(key string) (*tunnelConn, error)
	RemoveTunnel(key string, tc *tunnelConn)
}

// ProxyState 代理状态
type ProxyState struct {
	RemotePort int
	LocalPort  int
	LocalIP    string
	Protocol   string
	BindAddr   string
	Listener   io.Closer
}

// ProxyInfo 代理信息（用于 API 响应）
type ProxyInfo struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
	PublicAddr string `json:"public_addr"`
}

// ClientInfo 客户端信息（用于 API 响应）
type ClientInfo struct {
	ID          string `json:"id"`
	RemoteAddr  string `json:"remote_addr"`
	ConnectedAt int64  `json:"connected_at"`
	ProxyCount  int    `json:"proxy_count"`
	Host        string `json:"host"`
	Online      bool   `json:"online"`
}

// RelaySession 中继会话
type RelaySession struct {
	ID            string
	SourceClient  string
	TargetClient  string
	Protocol      string
	SourcePort    int
	TargetPort    int
	TargetLocalIP string
	SourceLocalIP string
	Token         string
	CreatedAt     time.Time
	Status        string // connecting, connected, failed, closed
	Error         string
}

// ==================== 状态机实现 ====================

// StateConfig 状态机配置
type StateConfig struct {
	PublicIP     string
	PingInterval time.Duration
}

// StateWorker 状态机 Worker (管理态)
//
// 只负责状态管理，低频、带锁、可阻塞：
// - 客户端连接状态
// - 代理状态
// - 隧道状态
// - 中继会话
type StateWorker struct {
	config StateConfig

	// 客户端状态
	clients   sync.Map // clientID -> *clientTable
	portIndex map[int]string
	portMu    sync.RWMutex

	// 中继会话
	relaySessions map[string]*RelaySession
	desiredRelays map[string]RelayDesiredState // 期望状态（持久化用）
	relayMu       sync.RWMutex

	// 代理期望状态（跨重连保留）
	desiredProxies map[string]ProxyDesiredState // key: "clientID:port"
	proxyMu        sync.RWMutex

// 持久化存储
	storage        *store.StorageManager
	dispatcher     Dispatcher
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// ProxyDesiredState proxy 期望状态
type ProxyDesiredState struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// NewStateWorker 创建状态机 Worker
func NewStateWorker(config StateConfig, dispatcher Dispatcher, storage *store.StorageManager) *StateWorker {
	return &StateWorker{
		config:         config,
		portIndex:      make(map[int]string),
		relaySessions:  make(map[string]*RelaySession),
		desiredRelays:  make(map[string]RelayDesiredState),
		desiredProxies: make(map[string]ProxyDesiredState),
		storage:        storage,
		stopCh:         make(chan struct{}),
		dispatcher:     dispatcher,
	}
}

// Name 返回 Worker 名称
func (w *StateWorker) Name() string {
	return "state"
}

// Run 启动状态机 Worker
func (w *StateWorker) Run() error {
	alog.Info(alog.CatSystem, "StateWorker 启动")

	// 等待停止信号
	<-w.stopCh
	alog.Info(alog.CatSystem, "StateWorker 停止")
	return nil
}

// Stop 停止状态机 Worker
func (w *StateWorker) Stop() {
	close(w.stopCh)
}

// ==================== StateStore 接口实现 ====================

// emitEvent 发布事件到 Dispatcher (异步触发)
func (w *StateWorker) emitEvent(event proto.GenericEvent) {
	if w.dispatcher != nil {
		w.dispatcher.Dispatch(event)
	}
}

// AddClient 添加客户端
func (w *StateWorker) AddClient(clientID string, conn ClientConn, remoteAddr string) {
	table := newClientTable(clientID, conn)
	table.remoteAddr = remoteAddr
	w.clients.Store(clientID, table)

	// 发布 client ADDED 事件
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeClient,
			Namespace: proto.NamespaceDefault,
			Name:      clientID,
		},
		proto.EventAdded,
		map[string]any{"remote_addr": remoteAddr, "host": conn.GetHost()},
	)
	w.emitEvent(event)
}

// RemoveClient 移除客户端
func (w *StateWorker) RemoveClient(clientID string) {
	val, ok := w.clients.Load(clientID)
	if !ok {
		return
	}
	table := val.(*clientTable)
	table.Cleanup()
	w.clients.Delete(clientID)

	// 清理端口索引
	w.portMu.Lock()
	for port, cid := range w.portIndex {
		if cid == clientID {
			delete(w.portIndex, port)
		}
	}
	w.portMu.Unlock()

	// 发布 client DELETED 事件
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeClient,
			Namespace: proto.NamespaceDefault,
			Name:      clientID,
		},
		proto.EventDeleted,
		nil,
	)
	w.emitEvent(event)
}

// GetClient 获取客户端状态
func (w *StateWorker) GetClient(clientID string) (ClientState, bool) {
	val, ok := w.clients.Load(clientID)
	if !ok {
		return nil, false
	}
	return val.(*clientTable), true
}

// ListClients 列出所有在线客户端
func (w *StateWorker) ListClients() []ClientInfo {
	var list []ClientInfo
	w.clients.Range(func(key, value any) bool {
		table := value.(*clientTable)
		list = append(list, ClientInfo{
			ID:          table.clientID,
			RemoteAddr:  table.remoteAddr,
			ConnectedAt: table.connectedAt,
			ProxyCount:  table.ProxyCount(),
			Host:        table.host,
			Online:      true,
		})
		return true
	})
	return list
}

// SendCommand 发送命令给客户端
func (w *StateWorker) SendCommand(clientID string, cmd any) error {
	val, ok := w.clients.Load(clientID)
	if !ok {
		return fmt.Errorf("client not found")
	}
	table := val.(*clientTable)
	return table.conn.WriteJSON(cmd)
}

// AddProxy 添加代理
func (w *StateWorker) AddProxy(clientID string, proxy ProxyState) {
	// 写入运行时状态
	val, ok := w.clients.Load(clientID)
	if !ok {
		return
	}
	table := val.(*clientTable)
	table.AddProxy(proxy)

	// 写入期望状态
	key := fmt.Sprintf("%s:%d", clientID, proxy.RemotePort)
	w.proxyMu.Lock()
	w.desiredProxies[key] = ProxyDesiredState{
		ClientID:   clientID,
		RemotePort: proxy.RemotePort,
		LocalPort:  proxy.LocalPort,
		LocalIP:    proxy.LocalIP,
		Protocol:   proxy.Protocol,
		BindAddr:   proxy.BindAddr,
	}
	w.proxyMu.Unlock()

	// 发布事件到 Dispatcher
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      fmt.Sprintf("%s:%d", clientID, proxy.RemotePort),
		},
		proto.EventAdded,
		proxy,
	)
	w.emitEvent(event)

	// 持久化事件
	if w.storage != nil {
		if err := w.storage.AppendEventAndSave(event, proxy); err != nil {
			alog.Error(alog.CatSystem, "持久化代理事件失败", "error", err)
		}
	}
}

// RemoveProxy 移除代理
func (w *StateWorker) RemoveProxy(clientID string, port int) {
	// 清理运行时状态
	val, ok := w.clients.Load(clientID)
	if !ok {
		return
	}
	table := val.(*clientTable)
	table.RemoveProxy(port)

	// 清理期望状态
	key := fmt.Sprintf("%s:%d", clientID, port)
	w.proxyMu.Lock()
	delete(w.desiredProxies, key)
	w.proxyMu.Unlock()

	// 发布事件到 Dispatcher
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      fmt.Sprintf("%s:%d", clientID, port),
		},
		proto.EventDeleted,
		nil,
	)
	w.emitEvent(event)

	// 持久化事件
	if w.storage != nil {
		if err := w.storage.DeleteResource(event.Resource); err != nil {
			alog.Error(alog.CatSystem, "持久化代理删除事件失败", "error", err)
		}
	}
}

// GetProxy 获取代理
func (w *StateWorker) GetProxy(clientID string, port int) (ProxyState, bool) {
	val, ok := w.clients.Load(clientID)
	if !ok {
		return ProxyState{}, false
	}
	table := val.(*clientTable)
	return table.GetProxy(port)
}

// ListProxies 列出所有代理
func (w *StateWorker) ListProxies() []ProxyInfo {
	publicIP := w.config.PublicIP
	var result []ProxyInfo
	w.clients.Range(func(_, value any) bool {
		table := value.(*clientTable)
		for _, p := range table.ListProxies() {
			var portStr string
			if p.Listener != nil {
				if ln, ok := p.Listener.(net.Listener); ok {
					_, portStr, _ = net.SplitHostPort(ln.Addr().String())
				}
			}
			if portStr == "" {
				portStr = fmt.Sprintf("%d", p.RemotePort)
			}
			result = append(result, ProxyInfo{
				ClientID:   table.clientID,
				RemotePort: p.RemotePort,
				LocalPort:  p.LocalPort,
				LocalIP:    p.LocalIP,
				Protocol:   p.Protocol,
				BindAddr:   p.BindAddr,
				PublicAddr: publicIP + ":" + portStr,
			})
		}
		return true
	})
	return result
}

// RegisterPort 注册端口映射
func (w *StateWorker) RegisterPort(clientID string, port int) {
	w.portMu.Lock()
	w.portIndex[port] = clientID
	w.portMu.Unlock()
}

// UnregisterPort 注销端口映射
func (w *StateWorker) UnregisterPort(port int) {
	w.portMu.Lock()
	delete(w.portIndex, port)
	w.portMu.Unlock()
}

// GetClientIDByPort 根据端口获取客户端 ID
func (w *StateWorker) GetClientIDByPort(port int) (string, bool) {
	w.portMu.RLock()
	defer w.portMu.RUnlock()
	clientID, ok := w.portIndex[port]
	return clientID, ok
}

// StoreTunnelToken 存储隧道令牌
func (w *StateWorker) StoreTunnelToken(token, key string) {
	// 遍历所有客户端，找到匹配的
	w.clients.Range(func(_, value any) bool {
		table := value.(*clientTable)
		table.StoreTunnelToken(token, key)
		return true
	})
}

// RemoveTunnelTokenByKey 根据 key 移除隧道令牌
func (w *StateWorker) RemoveTunnelTokenByKey(key string) {
	w.clients.Range(func(_, value any) bool {
		table := value.(*clientTable)
		table.RemoveTunnelTokenByKey(key)
		return true
	})
}

// FindTableByWSToken 根据 WS 令牌查找客户端
func (w *StateWorker) FindTableByWSToken(token string) (ClientState, string, error) {
	var found *clientTable
	var foundKey string
	w.clients.Range(func(_, value any) bool {
		table := value.(*clientTable)
		key, err := table.FindTableByWSToken(token)
		if err == nil {
			found = table
			foundKey = key
			return false
		}
		return true
	})
	if found == nil {
		return nil, "", fmt.Errorf("invalid token")
	}
	return found, foundKey, nil
}

// GetPublicIP 获取公网 IP
func (w *StateWorker) GetPublicIP() string {
	return w.config.PublicIP
}

// ==================== 中继操作 ====================

// CreateRelaySession 创建中继会话
func (w *StateWorker) CreateRelaySession(session RelaySession) {
	w.relayMu.Lock()
	w.relaySessions[session.ID] = &session
	// 同步更新期望状态
	w.desiredRelays[session.ID] = RelayDesiredState{
		ID:            session.ID,
		SourceClient:  session.SourceClient,
		TargetClient:  session.TargetClient,
		Protocol:      session.Protocol,
		SourcePort:    session.SourcePort,
		TargetPort:    session.TargetPort,
		TargetLocalIP: session.TargetLocalIP,
		SourceLocalIP: session.SourceLocalIP,
	}
	w.relayMu.Unlock()

	// 发布事件到 Dispatcher
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      session.ID,
		},
		proto.EventAdded,
		session,
	)
	w.emitEvent(event)

	// 持久化事件
	if w.storage != nil {
		if err := w.storage.AppendEventAndSave(event, session); err != nil {
			alog.Error(alog.CatSystem, "持久化中继事件失败", "error", err)
		}
	}
}

// GetRelaySession 获取中继会话
func (w *StateWorker) GetRelaySession(sessionID string) (RelaySession, bool) {
	w.relayMu.RLock()
	defer w.relayMu.RUnlock()
	session, ok := w.relaySessions[sessionID]
	if !ok {
		return RelaySession{}, false
	}
	return *session, true
}

// RemoveRelaySession 移除中继会话
func (w *StateWorker) RemoveRelaySession(sessionID string) {
	w.relayMu.Lock()
	delete(w.relaySessions, sessionID)
	delete(w.desiredRelays, sessionID)
	w.relayMu.Unlock()

	// 发布事件到 Dispatcher
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      sessionID,
		},
		proto.EventDeleted,
		nil,
	)
	w.emitEvent(event)

	// 持久化事件
	if w.storage != nil {
		if err := w.storage.DeleteResource(event.Resource); err != nil {
			alog.Error(alog.CatSystem, "持久化中继删除事件失败", "error", err)
		}
	}
}

// ListRelaySessions 列出所有中继会话
func (w *StateWorker) ListRelaySessions() []RelaySession {
	w.relayMu.RLock()
	defer w.relayMu.RUnlock()
	var sessions []RelaySession
	for _, s := range w.relaySessions {
		sessions = append(sessions, *s)
	}
	return sessions
}

// ==================== 内部客户端表 ====================

// clientTable 客户端状态表（内部实现）
type clientTable struct {
	clientID    string
	conn        ClientConn
	remoteAddr  string
	connectedAt int64
	host        string

	proxies map[int]*ProxyState
	mu      sync.RWMutex

	tunnels      map[string]*tunnelConn
	tunnelTokens map[string]string
	tunnelMu     sync.RWMutex
}

// newClientTable 创建客户端表
func newClientTable(clientID string, conn ClientConn) *clientTable {
	return &clientTable{
		clientID:     clientID,
		conn:         conn,
		connectedAt:  time.Now().Unix(),
		proxies:      make(map[int]*ProxyState),
		tunnels:      make(map[string]*tunnelConn),
		tunnelTokens: make(map[string]string),
	}
}

func (t *clientTable) ClientID() string   { return t.clientID }
func (t *clientTable) Conn() ClientConn   { return t.conn }
func (t *clientTable) RemoteAddr() string { return t.remoteAddr }
func (t *clientTable) ConnectedAt() int64 { return t.connectedAt }
func (t *clientTable) Host() string       { return t.host }
func (t *clientTable) SetHost(h string)   { t.host = h }

func (t *clientTable) TunnelHost(publicIP string) string {
	if t.host == "" {
		return publicIP
	}
	return t.host
}

func (t *clientTable) TunnelKey(port int) string {
	return fmt.Sprintf("%s-%d", t.clientID, port)
}

func (t *clientTable) AddProxy(p ProxyState) {
	t.mu.Lock()
	t.proxies[p.RemotePort] = &p
	t.mu.Unlock()
}

func (t *clientTable) RemoveProxy(port int) {
	t.mu.Lock()
	delete(t.proxies, port)
	t.mu.Unlock()
}

func (t *clientTable) GetProxy(port int) (ProxyState, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	p, ok := t.proxies[port]
	if !ok {
		return ProxyState{}, false
	}
	return *p, true
}

func (t *clientTable) GetProxyListener(port int) io.Closer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if p, ok := t.proxies[port]; ok {
		return p.Listener
	}
	return nil
}

func (t *clientTable) ListProxies() []ProxyState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	list := make([]ProxyState, 0, len(t.proxies))
	for _, p := range t.proxies {
		list = append(list, *p)
	}
	return list
}

func (t *clientTable) ProxyCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.proxies)
}

func (t *clientTable) StoreTunnelToken(token, key string) {
	t.tunnelMu.Lock()
	t.tunnelTokens[token] = key
	t.tunnelMu.Unlock()
}

func (t *clientTable) RemoveTunnelTokenByKey(key string) {
	t.tunnelMu.Lock()
	for token, k := range t.tunnelTokens {
		if k == key {
			delete(t.tunnelTokens, token)
		}
	}
	t.tunnelMu.Unlock()
}

func (t *clientTable) FindTableByWSToken(token string) (string, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	key, ok := t.tunnelTokens[token]
	if !ok {
		return "", fmt.Errorf("invalid token")
	}
	return key, nil
}

func (t *clientTable) Cleanup() {
	t.mu.Lock()
	for _, p := range t.proxies {
		if p.Listener != nil {
			p.Listener.Close()
		}
	}
	t.proxies = make(map[int]*ProxyState)
	t.mu.Unlock()

	t.tunnelMu.Lock()
	for _, tc := range t.tunnels {
		tc.Close()
	}
	t.tunnels = make(map[string]*tunnelConn)
	t.tunnelMu.Unlock()
}

// PutTunnel 保存隧道连接
func (t *clientTable) PutTunnel(key string, tc *tunnelConn) {
	t.tunnelMu.Lock()
	old := t.tunnels[key]
	t.tunnels[key] = tc
	t.tunnelMu.Unlock()
	if old != nil {
		old.Close()
	}
}

// GetTunnel 获取隧道连接
func (t *clientTable) GetTunnel(key string) (*tunnelConn, error) {
	t.tunnelMu.RLock()
	defer t.tunnelMu.RUnlock()
	tc, ok := t.tunnels[key]
	if !ok {
		return nil, fmt.Errorf("no tunnel for key %s", key)
	}
	return tc, nil
}

// RemoveTunnel 移除隧道连接
func (t *clientTable) RemoveTunnel(key string, tc *tunnelConn) {
	t.tunnelMu.Lock()
	if t.tunnels[key] == tc {
		delete(t.tunnels, key)
	}
	t.tunnelMu.Unlock()
}

// generateRandomToken 生成随机令牌
func generateRandomToken(length int) string {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}

// ==================== 持久化 Provider ====================

// RelayDesiredState relay 期望状态（用于持久化）
type RelayDesiredState struct {
	ID            string `json:"id"`
	SourceClient  string `json:"source_client"`
	TargetClient  string `json:"target_client"`
	Protocol      string `json:"protocol"`
	SourcePort    int    `json:"source_port"`
	TargetPort    int    `json:"target_port"`
	TargetLocalIP string `json:"target_local_ip"`
	SourceLocalIP string `json:"source_local_ip"`
}

// relayProvider relay 持久化 provider（内部实现）
type relayProvider struct {
	state *StateWorker
}

func (p *relayProvider) Name() string { return "relay" }

func (p *relayProvider) Export() any {
	p.state.relayMu.RLock()
	defer p.state.relayMu.RUnlock()

	var relays []RelayDesiredState
	for _, s := range p.state.desiredRelays {
		relays = append(relays, s)
	}
	return relays
}

func (p *relayProvider) Import(data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		return
	}
	var relays []RelayDesiredState
	if err := json.Unmarshal(raw, &relays); err != nil {
		return
	}

	p.state.relayMu.Lock()
	for _, r := range relays {
		p.state.desiredRelays[r.ID] = r
	}
	p.state.relayMu.Unlock()
}

// RegisterPersistProviders 注册所有持久化 provider
func RegisterPersistProviders(state *StateWorker) {
	registerRelayProviderGlobal(state)
	registerProxyProviderGlobal(state)
}

// proxyProvider proxy 持久化 provider（内部实现）
type proxyProvider struct {
	state *StateWorker
}

func (p *proxyProvider) Name() string { return "proxy" }

func (p *proxyProvider) Export() any {
	p.state.proxyMu.RLock()
	defer p.state.proxyMu.RUnlock()

	var proxies []ProxyDesiredState
	for _, d := range p.state.desiredProxies {
		proxies = append(proxies, d)
	}
	return proxies
}

func (p *proxyProvider) Import(data any) {
	raw, err := json.Marshal(data)
	if err != nil {
		return
	}
	var proxies []ProxyDesiredState
	if err := json.Unmarshal(raw, &proxies); err != nil {
		return
	}

	p.state.proxyMu.Lock()
	for _, d := range proxies {
		key := fmt.Sprintf("%s:%d", d.ClientID, d.RemotePort)
		p.state.desiredProxies[key] = d
	}
	p.state.proxyMu.Unlock()
}
