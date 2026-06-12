package runtime

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"fmt"
	"sync"
	"time"
)

// StateStoreImpl 状态存储实现 (CQRS Query Side)
//
// 所有状态修改只能通过 Apply(event) 方法。
// 这是唯一允许修改状态的入口。
type StateStoreImpl struct {
	mu sync.RWMutex

	// 客户端状态
	clients map[string]*clientEntry

	// 代理状态 (key: "clientID:port")
	proxies map[string]*ProxyState

	// 中继会话
	relays map[string]*RelaySession

	// 端口索引
	portIndex map[int]string // port -> clientID

	// 期望状态 (for Reconcile)
	desiredProxies map[string]ProxyDesiredState
	desiredRelays  map[string]RelayDesiredState

	// 隧道令牌索引
	tunnelTokens map[string]string // token -> "clientID-port"

	publicIP string
}

// clientEntry 客户端条目 (内部存储)
type clientEntry struct {
	ClientID    string
	Conn        ClientConn
	RemoteAddr  string
	ConnectedAt int64
	Host        string
	Latency     time.Duration
}

// NewStateStore 创建状态存储
func NewStateStore(publicIP string) *StateStoreImpl {
	return &StateStoreImpl{
		clients:        make(map[string]*clientEntry),
		proxies:        make(map[string]*ProxyState),
		relays:         make(map[string]*RelaySession),
		portIndex:      make(map[int]string),
		desiredProxies: make(map[string]ProxyDesiredState),
		desiredRelays:  make(map[string]RelayDesiredState),
		tunnelTokens:   make(map[string]string),
		publicIP:       publicIP,
	}
}

// ==================== Query Methods (只读) ====================

func (s *StateStoreImpl) GetClient(clientID string) (ClientState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.clients[clientID]
	if !ok {
		return nil, false
	}
	return &clientStateAdapter{entry: entry}, true
}

func (s *StateStoreImpl) GetClientInfo(clientID string) (ClientInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.clients[clientID]
	if !ok {
		return ClientInfo{}, false
	}
	proxyCount := 0
	for _, p := range s.proxies {
		if p.ClientID == entry.ClientID {
			proxyCount++
		}
	}
	return ClientInfo{
		ID:          entry.ClientID,
		RemoteAddr:  entry.RemoteAddr,
		ConnectedAt: entry.ConnectedAt,
		Host:        entry.Host,
		Online:      true,
		ProxyCount:  proxyCount,
	}, true
}

func (s *StateStoreImpl) ListClients() []ClientInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]ClientInfo, 0, len(s.clients))
	for _, entry := range s.clients {
		// 统计该客户端的代理数
		proxyCount := 0
		for _, p := range s.proxies {
			if p.ClientID == entry.ClientID {
				proxyCount++
			}
		}
		list = append(list, ClientInfo{
			ID:          entry.ClientID,
			RemoteAddr:  entry.RemoteAddr,
			ConnectedAt: entry.ConnectedAt,
			Host:        entry.Host,
			Online:      true,
			ProxyCount:  proxyCount,
		})
	}
	return list
}

func (s *StateStoreImpl) SendCommand(clientID string, cmd any) error {
	s.mu.RLock()
	entry, ok := s.clients[clientID]
	if !ok {
		s.mu.RUnlock()
		return fmt.Errorf("client not found: %s", clientID)
	}
	conn := entry.Conn
	s.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("client connection not ready: %s", clientID)
	}
	return conn.WriteJSON(cmd)
}

func (s *StateStoreImpl) GetProxy(clientID string, port int) (ProxyState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("%s:%d", clientID, port)
	p, ok := s.proxies[key]
	if !ok {
		return ProxyState{}, false
	}
	return *p, true
}

func (s *StateStoreImpl) ListProxies() []ProxyInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]ProxyInfo, 0, len(s.proxies))
	for _, p := range s.proxies {
		publicAddr := fmt.Sprintf("%s:%d", s.publicIP, p.RemotePort)
		result = append(result, ProxyInfo{
			ClientID:   p.ClientID,
			RemotePort: p.RemotePort,
			LocalPort:  p.LocalPort,
			LocalIP:    p.LocalIP,
			Protocol:   p.Protocol,
			BindAddr:   p.BindAddr,
			PublicAddr: publicAddr,
		})
	}
	return result
}

func (s *StateStoreImpl) GetClientIDByPort(port int) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	clientID, ok := s.portIndex[port]
	return clientID, ok
}

func (s *StateStoreImpl) GetRelaySession(sessionID string) (RelaySession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.relays[sessionID]
	if !ok {
		return RelaySession{}, false
	}
	return *session, true
}

func (s *StateStoreImpl) ListRelaySessions() []RelaySession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]RelaySession, 0, len(s.relays))
	for _, session := range s.relays {
		sessions = append(sessions, *session)
	}
	return sessions
}

func (s *StateStoreImpl) FindTableByWSToken(token string) (ClientState, string, error) {
	s.mu.RLock()
	key, ok := s.tunnelTokens[token]
	if !ok {
		s.mu.RUnlock()
		return nil, "", fmt.Errorf("invalid token")
	}
	// 解析 key: "clientID-port" -> clientID
	clientID := parseClientIDFromKey(key)
	entry, exists := s.clients[clientID]
	s.mu.RUnlock()
	if !exists {
		return nil, "", fmt.Errorf("client not found")
	}
	return &clientStateAdapter{entry: entry}, key, nil
}

func (s *StateStoreImpl) GetPublicIP() string {
	return s.publicIP
}

// SetClientConn 设置客户端连接
//
// 活连接不经过事件系统，直接设置。
func (s *StateStoreImpl) SetClientConn(clientID string, conn ClientConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.clients[clientID]
	if !ok {
		// 客户端不存在，创建一个新条目
		entry = &clientEntry{
			ClientID:    clientID,
			ConnectedAt: time.Now().Unix(),
		}
		s.clients[clientID] = entry
	}
	entry.Conn = conn
}

// ==================== Desired State (for Reconcile) ====================

func (s *StateStoreImpl) GetDesiredProxies() []ProxyDesiredState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]ProxyDesiredState, 0, len(s.desiredProxies))
	for _, d := range s.desiredProxies {
		if d.ClientID == "" || d.RemotePort == 0 {
			continue
		}
		result = append(result, d)
	}
	return result
}

func (s *StateStoreImpl) GetDesiredRelays() []RelayDesiredState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]RelayDesiredState, 0, len(s.desiredRelays))
	for _, d := range s.desiredRelays {
		if d.ID == "" {
			continue
		}
		result = append(result, d)
	}
	return result
}

// StoreTunnelToken 存储隧道令牌
func (s *StateStoreImpl) StoreTunnelToken(token, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tunnelTokens[token] = key
}

// RemoveTunnelToken 移除隧道令牌
func (s *StateStoreImpl) RemoveTunnelToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tunnelTokens, token)
}

// ==================== Apply (Event-driven) ====================

// Apply 应用事件到状态存储
//
// 这是唯一允许修改状态的方法。
// Runtime 禁止直接写 StateStore，只能 Emit(Event)。
func (s *StateStoreImpl) Apply(event proto.GenericEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Resource.Type {
	case proto.ResourceTypeClient:
		s.applyClientEvent(event)
	case proto.ResourceTypeProxy:
		alog.Info(alog.CatSystem, "StateStore Apply proxy", "event", string(event.EventType), "name", event.Resource.Name)
		s.applyProxyEvent(event)
	case proto.ResourceTypeRelay:
		s.applyRelayEvent(event)
	default:
		alog.Warn(alog.CatSystem, "Unknown event resource type",
			"type", event.Resource.Type,
			"event_id", event.ID,
		)
	}
}

// applyClientEvent 应用客户端事件
func (s *StateStoreImpl) applyClientEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		payload, ok := event.Payload.(map[string]any)
		if !ok {
			alog.Warn(alog.CatSystem, "Invalid client ADDED payload", "event_id", event.ID)
			return
		}
		clientID := event.Resource.Name
		remoteAddr, _ := payload["remote_addr"].(string)
		host, _ := payload["host"].(string)

		// 如果已有 entry 且有 Conn，保留 Conn (避免覆盖 SetClientConn 设置的连接)
		if existing, exists := s.clients[clientID]; exists && existing.Conn != nil {
			existing.RemoteAddr = remoteAddr
			existing.Host = host
		} else {
			s.clients[clientID] = &clientEntry{
				ClientID:    clientID,
				RemoteAddr:  remoteAddr,
				ConnectedAt: time.Now().Unix(),
				Host:        host,
			}
		}

	case proto.EventModified:
		clientID := event.Resource.Name
		entry, ok := s.clients[clientID]
		if !ok {
			return
		}
		if payload, ok := event.Payload.(map[string]any); ok {
			if host, ok := payload["host"].(string); ok {
				entry.Host = host
			}
			switch v := payload["latency"].(type) {
			case int64:
				entry.Latency = time.Duration(v) * time.Millisecond
			case float64:
				entry.Latency = time.Duration(v) * time.Millisecond
			}
		}

	case proto.EventDeleted:
		clientID := event.Resource.Name
		// 清理端口索引
		for port, cid := range s.portIndex {
			if cid == clientID {
				delete(s.portIndex, port)
			}
		}
		// 清理隧道令牌
		for token, key := range s.tunnelTokens {
			if parseClientIDFromKey(key) == clientID {
				delete(s.tunnelTokens, token)
			}
		}
		delete(s.clients, clientID)
	}
}

// applyProxyEvent 应用代理事件
func (s *StateStoreImpl) applyProxyEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		proxy := parseProxyFromPayload(event.Payload)
		if proxy == nil {
			alog.Error(alog.CatSystem, "Proxy ADDED 但解析失败! payload 类型丢失",
				"event_id", event.ID,
				"payload_type", fmt.Sprintf("%T", event.Payload),
				"resource", event.Resource.String(),
			)
			return
		}
		key := proxyKey(proxy.ClientID, proxy.RemotePort)
		s.proxies[key] = proxy
		s.portIndex[proxy.RemotePort] = proxy.ClientID

		// 同步期望状态
		s.desiredProxies[key] = ProxyDesiredState{
			ClientID:   proxy.ClientID,
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
		}

		alog.Info(alog.CatSystem, "Proxy ADDED", "key", key, "client_id", proxy.ClientID, "port", proxy.RemotePort)

	case proto.EventDeleted:
		key := event.Resource.Name
		alog.Warn(alog.CatSystem, "Proxy DELETED", "key", key, "event_id", event.ID)
		if proxy, ok := s.proxies[key]; ok {
			delete(s.portIndex, proxy.RemotePort)
		}
		delete(s.proxies, key)
		delete(s.desiredProxies, key)
	}
}

// applyRelayEvent 应用中继事件
func (s *StateStoreImpl) applyRelayEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		session, ok := event.Payload.(RelaySession)
		if !ok {
			alog.Warn(alog.CatSystem, "Invalid relay ADDED payload", "event_id", event.ID)
			return
		}
		s.relays[session.ID] = &session
		s.desiredRelays[session.ID] = RelayDesiredState{
			ID:            session.ID,
			SourceClient:  session.SourceClient,
			TargetClient:  session.TargetClient,
			Protocol:      session.Protocol,
			SourcePort:    session.SourcePort,
			TargetPort:    session.TargetPort,
			TargetLocalIP: session.TargetLocalIP,
			SourceLocalIP: session.SourceLocalIP,
			Token:         session.Token,
		}

	case proto.EventDeleted:
		sessionID := event.Resource.Name
		delete(s.relays, sessionID)
		delete(s.desiredRelays, sessionID)
	}
}

// ==================== Snapshot / Restore ====================

func (s *StateStoreImpl) Snapshot() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 深拷贝，避免外部修改内部状态
	proxiesCopy := make(map[string]ProxyDesiredState, len(s.desiredProxies))
	for k, v := range s.desiredProxies {
		proxiesCopy[k] = v
	}
	relaysCopy := make(map[string]RelayDesiredState, len(s.desiredRelays))
	for k, v := range s.desiredRelays {
		relaysCopy[k] = v
	}
	return map[string]any{
		"desired_proxies": proxiesCopy,
		"desired_relays":  relaysCopy,
	}
}

func (s *StateStoreImpl) Restore(data map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if proxies, ok := data["desired_proxies"].(map[string]any); ok {
		for key, val := range proxies {
			if m, ok := val.(map[string]any); ok {
				proxy := ProxyDesiredState{}
				if v, ok := m["client_id"].(string); ok {
					proxy.ClientID = v
				}
				if v, ok := m["remote_port"].(float64); ok {
					proxy.RemotePort = int(v)
				}
				if v, ok := m["local_port"].(float64); ok {
					proxy.LocalPort = int(v)
				}
				if v, ok := m["local_ip"].(string); ok {
					proxy.LocalIP = v
				}
				if v, ok := m["protocol"].(string); ok {
					proxy.Protocol = v
				}
				if v, ok := m["bind_addr"].(string); ok {
					proxy.BindAddr = v
				}
				if proxy.ClientID == "" || proxy.RemotePort == 0 {
					continue
				}
				s.desiredProxies[key] = proxy
				actualKey := proxyKey(proxy.ClientID, proxy.RemotePort)
				s.proxies[actualKey] = &ProxyState{
					ClientID:   proxy.ClientID,
					RemotePort: proxy.RemotePort,
					LocalPort:  proxy.LocalPort,
					LocalIP:    proxy.LocalIP,
					Protocol:   proxy.Protocol,
					BindAddr:   proxy.BindAddr,
				}
				s.portIndex[proxy.RemotePort] = proxy.ClientID
			}
		}
	}

	if relays, ok := data["desired_relays"].(map[string]any); ok {
		for key, val := range relays {
			if m, ok := val.(map[string]any); ok {
				relay := RelayDesiredState{}
				if v, ok := m["id"].(string); ok {
					relay.ID = v
				} else if v, ok := m["session_id"].(string); ok {
					relay.ID = v
				}
				if v, ok := m["source_client"].(string); ok {
					relay.SourceClient = v
				}
				if v, ok := m["target_client"].(string); ok {
					relay.TargetClient = v
				}
				if v, ok := m["protocol"].(string); ok {
					relay.Protocol = v
				}
				if v, ok := m["source_port"].(float64); ok {
					relay.SourcePort = int(v)
				}
				if v, ok := m["target_port"].(float64); ok {
					relay.TargetPort = int(v)
				}
				if v, ok := m["target_local_ip"].(string); ok {
					relay.TargetLocalIP = v
				}
				if v, ok := m["source_local_ip"].(string); ok {
					relay.SourceLocalIP = v
				}
				if v, ok := m["token"].(string); ok {
					relay.Token = v
				}
				if relay.ID == "" {
					continue
				}
				s.desiredRelays[key] = relay
				s.relays[relay.ID] = &RelaySession{
					ID:            relay.ID,
					SourceClient:  relay.SourceClient,
					TargetClient:  relay.TargetClient,
					Protocol:      relay.Protocol,
					SourcePort:    relay.SourcePort,
					TargetPort:    relay.TargetPort,
					TargetLocalIP: relay.TargetLocalIP,
					SourceLocalIP: relay.SourceLocalIP,
					Token:         relay.Token,
				}
			}
		}
	}

	alog.Info(alog.CatSystem, "StateStore 快照恢复完成",
		"proxies", len(s.proxies),
		"relays", len(s.relays),
	)
}

// ==================== Helpers ====================

func proxyKey(clientID string, port int) string {
	return fmt.Sprintf("%s:%d", clientID, port)
}

func parseClientIDFromKey(key string) string {
	// key 格式: "clientID-port" 或 "clientID:port"
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '-' || key[i] == ':' {
			return key[:i]
		}
	}
	return key
}

func parseProxyFromPayload(payload any) *ProxyState {
	switch p := payload.(type) {
	case ProxyState:
		return &p
	case map[string]any:
		proxy := &ProxyState{}
		if v, ok := p["client_id"].(string); ok {
			proxy.ClientID = v
		}
		if v, ok := p["remote_port"].(float64); ok {
			proxy.RemotePort = int(v)
		}
		if v, ok := p["local_port"].(float64); ok {
			proxy.LocalPort = int(v)
		}
		if v, ok := p["local_ip"].(string); ok {
			proxy.LocalIP = v
		}
		if v, ok := p["protocol"].(string); ok {
			proxy.Protocol = v
		}
		if v, ok := p["bind_addr"].(string); ok {
			proxy.BindAddr = v
		}
		return proxy
	default:
		return nil
	}
}

// ==================== ClientState Adapter ====================

// clientStateAdapter 将 clientEntry 适配为 ClientState 接口
type clientStateAdapter struct {
	entry *clientEntry
}

func (a *clientStateAdapter) ClientID() string   { return a.entry.ClientID }
func (a *clientStateAdapter) Conn() ClientConn   { return a.entry.Conn }
func (a *clientStateAdapter) RemoteAddr() string { return a.entry.RemoteAddr }
func (a *clientStateAdapter) ConnectedAt() int64 { return a.entry.ConnectedAt }
func (a *clientStateAdapter) Host() string       { return a.entry.Host }
func (a *clientStateAdapter) SetHost(h string)   { a.entry.Host = h }
func (a *clientStateAdapter) TunnelHost(publicIP string) string {
	if a.entry.Host == "" {
		return publicIP
	}
	return a.entry.Host
}
func (a *clientStateAdapter) TunnelKey(port int) string {
	return fmt.Sprintf("%s-%d", a.entry.ClientID, port)
}
func (a *clientStateAdapter) AddProxy(p ProxyState)                {}
func (a *clientStateAdapter) RemoveProxy(port int)                 {}
func (a *clientStateAdapter) GetProxy(port int) (ProxyState, bool) { return ProxyState{}, false }
func (a *clientStateAdapter) ListProxies() []ProxyState            { return nil }
func (a *clientStateAdapter) ProxyCount() int                      { return 0 }
func (a *clientStateAdapter) StoreTunnelToken(token, key string)   {}
func (a *clientStateAdapter) RemoveTunnelTokenByKey(key string)    {}
func (a *clientStateAdapter) FindTableByWSToken(token string) (string, error) {
	return "", fmt.Errorf("not implemented")
}
func (a *clientStateAdapter) Cleanup() {}
