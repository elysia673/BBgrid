package runtime

import (
	"BBgrid/common/proto"
	"io"
	"net"
	"time"
)

// ==================== EventBus ====================

// EventHandler 事件处理函数
type EventHandler func(proto.GenericEvent)

// EventBus 事件总线接口
//
// 所有状态变更必须通过 EventBus 发布。
// 订阅者按 resourceType 路由，保证有序投递。
type EventBus interface {
	// Publish 发布事件到总线
	// 事件会按 resourceType 路由到所有匹配的订阅者
	Publish(event proto.GenericEvent)

	// Subscribe 按资源类型订阅事件
	Subscribe(resourceType string, handler EventHandler)

	// Unsubscribe 取消订阅
	Unsubscribe(resourceType string, handler EventHandler)

	// Close 关闭事件总线
	Close()
}

// ==================== StateStore ====================

// StateStore 状态存储接口 (CQRS Query Side)
//
// 只读查询 + event-driven apply。
// 禁止直接写入，只能通过 Apply(event) 修改状态。
type StateStore interface {
	// === 只读查询 ===

	GetClient(clientID string) (ClientState, bool)
	GetClientInfo(clientID string) (ClientInfo, bool)
	ListClients() []ClientInfo
	SendCommand(clientID string, cmd any) error

	GetProxy(clientID string, port int) (ProxyState, bool)
	ListProxies() []ProxyInfo
	GetClientIDByPort(port int) (string, bool)

	GetRelaySession(sessionID string) (RelaySession, bool)
	ListRelaySessions() []RelaySession

	FindTableByWSToken(token string) (ClientState, string, error)
	GetPublicIP() string

	// === Connection Management ===

	// SetClientConn 设置客户端连接 (活连接，不经过事件)
	SetClientConn(clientID string, conn ClientConn)

	// === Event-driven Apply ===

	// Apply 应用事件到状态存储
	// 这是唯一允许修改状态的方法
	Apply(event proto.GenericEvent)

	// === Desired State (for Reconcile) ===

	GetDesiredProxies() []ProxyDesiredState
	GetDesiredRelays() []RelayDesiredState

	// === Lifecycle ===

	// Snapshot 导出当前状态快照
	Snapshot() map[string]any

	// Restore 从快照恢复状态
	Restore(data map[string]any)
}

// ==================== Client Types ====================

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
	ListProxies() []ProxyState
	ProxyCount() int
	StoreTunnelToken(token, key string)
	RemoveTunnelTokenByKey(key string)
	FindTableByWSToken(token string) (string, error)
	Cleanup()
}

// ClientInfo 客户端信息 (API 响应)
type ClientInfo struct {
	ID          string `json:"id"`
	RemoteAddr  string `json:"remote_addr"`
	ConnectedAt int64  `json:"connected_at"`
	ProxyCount  int    `json:"proxy_count"`
	Host        string `json:"host"`
	Online      bool   `json:"online"`
}

// ==================== Proxy Types ====================

// ProxyState 代理状态
type ProxyState struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// ProxyInfo 代理信息 (API 响应)
type ProxyInfo struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
	PublicAddr string `json:"public_addr"`
}

// ProxyDesiredState proxy 期望状态 (持久化)
type ProxyDesiredState struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// ==================== Relay Types ====================

// RelaySession 中继会话
type RelaySession struct {
	ID            string    `json:"session_id"`
	SourceClient  string    `json:"source_client"`
	TargetClient  string    `json:"target_client"`
	Protocol      string    `json:"protocol"`
	SourcePort    int       `json:"source_port"`
	TargetPort    int       `json:"target_port"`
	TargetLocalIP string    `json:"target_local_ip"`
	SourceLocalIP string    `json:"source_local_ip"`
	Token         string    `json:"token,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	Status        string    `json:"status"`
	Error         string    `json:"error,omitempty"`
}

// RelayDesiredState relay 期望状态 (持久化)
type RelayDesiredState struct {
	ID            string `json:"id"`
	SourceClient  string `json:"source_client"`
	TargetClient  string `json:"target_client"`
	Protocol      string `json:"protocol"`
	SourcePort    int    `json:"source_port"`
	TargetPort    int    `json:"target_port"`
	TargetLocalIP string `json:"target_local_ip"`
	SourceLocalIP string `json:"source_local_ip"`
	Token         string `json:"token,omitempty"`
}

// ==================== Reconciler ====================

// Reconciler 状态协调器接口
//
// 负责保证 desired state == actual state。
// 属于 Runtime Core 内置机制，不是 plugin。
type Reconciler interface {
	// Trigger 手动触发协调
	Trigger()

	// Run 启动协调循环 (阻塞)
	Run() error

	// Stop 停止协调器
	Stop()

	// GetLastResult 获取最近一次协调结果
	GetLastResult() *ReconcileResult
}

// ReconcileResult 协调结果
type ReconcileResult struct {
	Timestamp     time.Time
	ProxiesTotal  int
	ProxiesFixed  int
	ProxiesFailed int
	RelaysTotal   int
	RelaysFixed   int
	RelaysFailed  int
	Duration      time.Duration
}

// ==================== Capability Registry ====================

// Capability 能力声明
type Capability struct {
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	Source      CapabilitySource `json:"source,omitempty"` // "internal" or "external"
	Schema      ActionSchema     `json:"schema,omitempty"`
}

// CapabilitySource 能力来源
type CapabilitySource string

const (
	SourceInternal CapabilitySource = "internal"
	SourceExternal CapabilitySource = "external"
)

// ActionSchema Action 的类型化元数据
type ActionSchema struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Params      []ParamSchema `json:"params"`
}

// ParamSchema 参数定义
type ParamSchema struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Default     string `json:"default,omitempty"`
	Description string `json:"description,omitempty"`
}

// ActionHandler Action 处理函数
type ActionHandler func(ctx *ActionContext) (*ActionResult, error)

// ActionContext Action 执行上下文
type ActionContext struct {
	Action  string
	Params  map[string]any
	Headers map[string]string
}

// ActionResult Action 执行结果
type ActionResult struct {
	Code     int          `json:"code"`
	Msg      string       `json:"msg"`
	Data     any          `json:"data,omitempty"`
	Body     io.ReadCloser `json:"-"`
	BodyName string       `json:"-"`
	BodySize int64        `json:"-"`
}

// CapabilityRegistry 能力注册表接口
type CapabilityRegistry interface {
	// Register 注册能力
	Register(capability Capability, handler ActionHandler)

	// Unregister 注销能力
	Unregister(name string)

	// Get 获取能力
	Get(name string) (Capability, ActionHandler, bool)

	// List 列出所有能力
	List() []Capability

	// Has 检查能力是否存在
	Has(name string) bool

	// Execute 执行 Action
	Execute(ctx *ActionContext) (*ActionResult, error)

	// RegisterProxyProvider 注册代理提供者
	RegisterProxyProvider(name string, provider ProxyProvider)

	// GetProxyProvider 获取代理提供者
	GetProxyProvider(name string) (ProxyProvider, bool)

	// RegisterRelayProvider 注册中继提供者
	RegisterRelayProvider(name string, provider RelayProvider)

	// GetRelayProvider 获取中继提供者
	GetRelayProvider(name string) (RelayProvider, bool)
}

// ==================== Reconcile Provider ====================

// ReconcileProvider 协调提供者接口
//
// Plugin 实现此接口，Reconcile Engine 调用它来创建/删除资源。
type ReconcileProvider interface {
	// CreateProxy 创建代理
	CreateProxy(clientID string, proxy ProxyState) error

	// DeleteProxy 删除代理
	DeleteProxy(clientID string, port int) error

	// CreateRelay 创建中继
	CreateRelay(session RelaySession) error

	// DeleteRelay 删除中继
	DeleteRelay(sessionID string) error
}

// ==================== Proxy / Relay Provider ====================

// ProxyCreateRequest 创建代理请求
type ProxyCreateRequest struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// ProxyProvider 代理生命周期管理接口
//
// 负责：启动/停止 listener，通知客户端，隧道配对。
// Session Layer 通过此接口操作代理，不再直接管理 listener。
type ProxyProvider interface {
	// Create 创建代理：启动 listener，通知客户端
	Create(req ProxyCreateRequest) error

	// Delete 删除代理：关闭 listener，通知客户端
	Delete(clientID string, port int) error

	// List 列出所有代理
	List() []ProxyInfo

	// GetListener 获取指定代理的 listener（供 Session Layer 接受隧道连接）
	GetListener(clientID string, port int) (net.Listener, bool)

	// Name 返回 provider 名称
	Name() string
}

// RelayCreateRequest 创建中继请求
type RelayCreateRequest struct {
	SourceClient  string `json:"source_client"`
	TargetClient  string `json:"target_client"`
	Protocol      string `json:"protocol"`
	SourcePort    int    `json:"source_port"`
	TargetPort    int    `json:"target_port"`
	TargetLocalIP string `json:"target_local_ip"`
	SourceLocalIP string `json:"source_local_ip"`
}

// RelayProvider 中继生命周期管理接口
//
// 负责：通知客户端，relay WS 桥接，隧道配对。
type RelayProvider interface {
	// Create 创建中继会话：通知源/目标客户端，返回 sessionID 和 token
	Create(req RelayCreateRequest) (sessionID string, token string, err error)

	// Delete 删除中继会话：通知客户端关闭
	Delete(sessionID string) error

	// List 列出所有中继会话
	List() []RelaySession

	// Name 返回 provider 名称
	Name() string
}
