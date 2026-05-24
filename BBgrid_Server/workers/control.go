package workers

import (
	alog "BBgrid/common/log"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// ==================== 接口定义 ====================

// Controller 控制器接口
//
// 定义控制器的核心能力，未来可以用 gRPC 实现。
type Controller interface {
	// 代理管理
	CreateProxy(req CreateProxyRequest) (*CreateProxyResponse, error)
	CloseProxy(port int) error
	ListProxies() []ProxyInfo

	// 客户端管理
	ListClients() []ClientInfo
	GetClientInfo(clientID string) (*ClientDetailInfo, error)

	// 中继管理
	CreateRelay(req CreateRelayRequest) (*CreateRelayResponse, error)
	CloseRelay(sessionID string) error
	ListRelays() []RelaySession

	// 命名空间管理
	ListNamespaces() []NamespaceInfo
	GetNamespace(name string) (*NamespaceInfo, error)
	GetNamespaceClients(namespace string) []ClientRecord
	SetClientNamespace(clientID, namespace, role string) error
}

// CreateProxyRequest 创建代理请求
type CreateProxyRequest struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// CreateProxyResponse 创建代理响应
type CreateProxyResponse struct {
	PublicAddr string `json:"public_addr"`
}

// CreateRelayRequest 创建中继请求
type CreateRelayRequest struct {
	SourceClientID string `json:"source_client_id"`
	TargetClientID string `json:"target_client_id"`
	SourcePort     int    `json:"source_port"`
	TargetPort     int    `json:"target_port"`
	Protocol       string `json:"protocol"`
	TargetLocalIP  string `json:"target_local_ip"`
	SourceLocalIP  string `json:"source_local_ip"`
}

// CreateRelayResponse 创建中继响应
type CreateRelayResponse struct {
	SessionID    string `json:"session_id"`
	SourceClient string `json:"source_client"`
	TargetClient string `json:"target_client"`
	Protocol     string `json:"protocol"`
}

// ClientDetailInfo 客户端详细信息
type ClientDetailInfo struct {
	ClientID string       `json:"client_id"`
	Ports    []ProxyState `json:"ports"`
}

// ==================== 控制器实现 ====================

// ControlConfig 控制器配置
type ControlConfig struct {
	Domain     string
	TunnelPort int
	APIKey     string
}

// ControlWorker 控制器 Worker (管理态)
//
// 实现 Controller 接口，处理 REST API 请求。
// 通过接口访问状态和注册表，不直接依赖具体实现。
type ControlWorker struct {
	config    ControlConfig
	state     StateStore
	registry  ClientRegistry
	nsManager NamespaceManager
	data      *DataWorker
	stopCh    chan struct{}
}

// NewControlWorker 创建控制器 Worker
func NewControlWorker(config ControlConfig, state StateStore, registry ClientRegistry, nsManager NamespaceManager, data *DataWorker) *ControlWorker {
	return &ControlWorker{
		config:    config,
		state:     state,
		registry:  registry,
		nsManager: nsManager,
		data:      data,
		stopCh:    make(chan struct{}),
	}
}

// Name 返回 Worker 名称
func (w *ControlWorker) Name() string {
	return "control"
}

// Run 启动控制器 Worker
func (w *ControlWorker) Run() error {
	alog.Info(alog.CatSystem, "ControlWorker 启动")

	// 等待停止信号
	<-w.stopCh
	alog.Info(alog.CatSystem, "ControlWorker 停止")
	return nil
}

// Stop 停止控制器 Worker
func (w *ControlWorker) Stop() {
	close(w.stopCh)
}

// ==================== Controller 接口实现 ====================

// CreateProxy 创建代理
func (w *ControlWorker) CreateProxy(req CreateProxyRequest) (*CreateProxyResponse, error) {
	// 验证客户端存在
	table, ok := w.state.GetClient(req.ClientID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}

	// 生成令牌
	token := generateToken()

	// 构建命令
	tunnelHost := table.TunnelHost(w.state.GetPublicIP())
	cmdData, err := json.Marshal(map[string]any{
		"remote_port": req.RemotePort,
		"local_port":  req.LocalPort,
		"protocol":    req.Protocol,
		"bind_addr":   req.BindAddr,
		"server_host": tunnelHost,
		"tunnel_port": w.config.TunnelPort,
		"token":       token,
		"local_ip":    req.LocalIP,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal command: %w", err)
	}

	cmd := map[string]any{
		"type": "proxy",
		"data": string(cmdData),
	}

	// 发送命令
	if err := w.state.SendCommand(req.ClientID, cmd); err != nil {
		return nil, fmt.Errorf("send command: %w", err)
	}

	// 更新状态
	w.state.AddProxy(req.ClientID, ProxyState{
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		LocalIP:    req.LocalIP,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
	})
	w.state.RegisterPort(req.ClientID, req.RemotePort)

	// 存储隧道令牌
	key := fmt.Sprintf("%s-%d", req.ClientID, req.RemotePort)
	w.state.StoreTunnelToken(token, key)

	// 启动代理监听 (通过 DataWorker)
	if req.Protocol == "tcp" {
		go w.data.StartTCPProxy(req.RemotePort, req.BindAddr, req.ClientID, token)
	}

	// 计算公共地址
	publicAddr := w.config.Domain
	if publicAddr == "" {
		publicAddr = w.state.GetPublicIP()
	}
	publicAddr = publicAddr + ":" + strconv.Itoa(req.RemotePort)

	alog.Info(alog.CatProxy, "代理已创建",
		"clientID", req.ClientID,
		"remotePort", req.RemotePort,
		"publicAddr", publicAddr)

	return &CreateProxyResponse{PublicAddr: publicAddr}, nil
}

// CloseProxy 关闭代理
func (w *ControlWorker) CloseProxy(port int) error {
	clientID, ok := w.state.GetClientIDByPort(port)
	if !ok {
		return fmt.Errorf("proxy not found")
	}

	key := fmt.Sprintf("%s-%d", clientID, port)

	// 通知客户端关闭
	notifyMsg := map[string]any{
		"type": "proxy_closed",
		"data": map[string]string{"key": key},
	}
	if err := w.state.SendCommand(clientID, notifyMsg); err != nil {
		alog.Warn(alog.CatProxy, "通知客户端关闭代理失败", "error", err)
	}

	// 关闭 listener
	w.data.CloseListener(key)

	// 清理状态
	w.state.RemoveTunnelTokenByKey(key)
	w.state.UnregisterPort(port)
	w.state.RemoveProxy(clientID, port)

	alog.Info(alog.CatProxy, "代理已关闭", "port", port)

	return nil
}

// ListProxies 列出所有代理
func (w *ControlWorker) ListProxies() []ProxyInfo {
	return w.state.ListProxies()
}

// ListClients 列出所有客户端（已注册 + 在线状态）
func (w *ControlWorker) ListClients() []ClientInfo {
	// 获取在线客户端
	onlineClients := w.state.ListClients()
	onlineMap := make(map[string]ClientInfo)
	for _, c := range onlineClients {
		onlineMap[c.ID] = c
	}

	// 获取已注册客户端
	approved := w.registry.GetApproved()

	var result []ClientInfo
	for _, record := range approved {
		if online, ok := onlineMap[record.ClientID]; ok {
			// 在线
			result = append(result, online)
		} else {
			// 离线
			result = append(result, ClientInfo{
				ID:     record.ClientID,
				Online: false,
			})
		}
	}

	return result
}

// GetClientInfo 获取客户端详情
func (w *ControlWorker) GetClientInfo(clientID string) (*ClientDetailInfo, error) {
	table, ok := w.state.GetClient(clientID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}

	proxies := table.ListProxies()
	return &ClientDetailInfo{
		ClientID: clientID,
		Ports:    proxies,
	}, nil
}

// CreateRelay 创建中继
func (w *ControlWorker) CreateRelay(req CreateRelayRequest) (*CreateRelayResponse, error) {
	// 验证客户端存在
	if _, ok := w.state.GetClient(req.SourceClientID); !ok {
		return nil, fmt.Errorf("source client not found")
	}
	if _, ok := w.state.GetClient(req.TargetClientID); !ok {
		return nil, fmt.Errorf("target client not found")
	}

	// 生成会话 ID 和令牌
	sessionID := generateToken()
	token := generateToken()

	// 设置默认值
	targetLocalIP := req.TargetLocalIP
	if targetLocalIP == "" {
		targetLocalIP = "127.0.0.1"
	}
	sourceLocalIP := req.SourceLocalIP
	if sourceLocalIP == "" {
		sourceLocalIP = "0.0.0.0"
	}

	// 创建会话
	session := RelaySession{
		ID:            sessionID,
		SourceClient:  req.SourceClientID,
		TargetClient:  req.TargetClientID,
		Protocol:      req.Protocol,
		SourcePort:    req.SourcePort,
		TargetPort:    req.TargetPort,
		TargetLocalIP: targetLocalIP,
		SourceLocalIP: sourceLocalIP,
		Token:         token,
		CreatedAt:     time.Now(),
		Status:        "connecting",
	}
	w.state.CreateRelaySession(session)

	// 发送信令给源客户端
	serverHost := w.config.Domain
	if serverHost == "" {
		serverHost = w.state.GetPublicIP()
	}

	sourceSignal := map[string]any{
		"type": "relay_signal",
		"data": map[string]any{
			"session_id":      sessionID,
			"protocol":        req.Protocol,
			"role":            "source",
			"peer_client_id":  req.TargetClientID,
			"source_port":     req.SourcePort,
			"target_port":     req.TargetPort,
			"target_local_ip": targetLocalIP,
			"source_local_ip": sourceLocalIP,
			"server_host":     serverHost,
			"token":           token,
		},
	}

	if err := w.state.SendCommand(req.SourceClientID, sourceSignal); err != nil {
		w.state.RemoveRelaySession(sessionID)
		return nil, fmt.Errorf("send signal to source: %w", err)
	}

	// 发送信令给目标客户端
	targetSignal := map[string]any{
		"type": "relay_signal",
		"data": map[string]any{
			"session_id":      sessionID,
			"protocol":        req.Protocol,
			"role":            "target",
			"peer_client_id":  req.SourceClientID,
			"source_port":     req.SourcePort,
			"target_port":     req.TargetPort,
			"target_local_ip": targetLocalIP,
			"source_local_ip": sourceLocalIP,
			"server_host":     serverHost,
			"token":           token,
		},
	}

	if err := w.state.SendCommand(req.TargetClientID, targetSignal); err != nil {
		w.state.RemoveRelaySession(sessionID)
		return nil, fmt.Errorf("send signal to target: %w", err)
	}

	alog.Info(alog.CatRelay, "中继会话已创建",
		"sessionID", sessionID,
		"source", req.SourceClientID,
		"target", req.TargetClientID)

	return &CreateRelayResponse{
		SessionID:    sessionID,
		SourceClient: req.SourceClientID,
		TargetClient: req.TargetClientID,
		Protocol:     req.Protocol,
	}, nil
}

// CloseRelay 关闭中继
func (w *ControlWorker) CloseRelay(sessionID string) error {
	w.state.RemoveRelaySession(sessionID)
	alog.Info(alog.CatRelay, "中继会话已关闭", "sessionID", sessionID)

	return nil
}

// ListRelays 列出所有中继
func (w *ControlWorker) ListRelays() []RelaySession {
	return w.state.ListRelaySessions()
}

// ListNamespaces 列出所有命名空间
func (w *ControlWorker) ListNamespaces() []NamespaceInfo {
	if w.nsManager == nil {
		return nil
	}
	ns := w.nsManager.ListNamespaces()
	result := make([]NamespaceInfo, len(ns))
	for i, n := range ns {
		result[i] = NamespaceInfo{
			Name:        n.Name,
			Description: n.Description,
			Type:        n.Type,
			Clients:     n.Clients,
			CreatedAt:   n.CreatedAt,
		}
	}
	return result
}

// GetNamespace 获取命名空间
func (w *ControlWorker) GetNamespace(name string) (*NamespaceInfo, error) {
	if w.nsManager == nil {
		return nil, fmt.Errorf("namespace manager not available")
	}
	ns := w.nsManager.GetNamespace(name)
	if ns == nil {
		return nil, fmt.Errorf("namespace not found")
	}
	return &NamespaceInfo{
		Name:        ns.Name,
		Description: ns.Description,
		Type:        ns.Type,
		Clients:     ns.Clients,
		CreatedAt:   ns.CreatedAt,
	}, nil
}

// GetNamespaceClients 获取命名空间下的客户端
func (w *ControlWorker) GetNamespaceClients(namespace string) []ClientRecord {
	if w.nsManager == nil {
		return nil
	}
	records := w.nsManager.GetClientsByNamespace(namespace)
	result := make([]ClientRecord, len(records))
	for i, r := range records {
		result[i] = ClientRecord{
			ClientID:   r.ClientID,
			Namespace:  r.Namespace,
			Role:       r.Role,
			ApprovedAt: r.ApprovedAt,
		}
	}
	return result
}

// SetClientNamespace 设置客户端命名空间
func (w *ControlWorker) SetClientNamespace(clientID, namespace, role string) error {
	if w.nsManager == nil {
		return fmt.Errorf("namespace manager not available")
	}
	return w.nsManager.SetClientNamespace(clientID, namespace, role)
}

// ==================== 辅助函数 ====================

func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}
