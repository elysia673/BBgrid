// Package manager 提供客户端连接和代理管理
//
// 负责维护客户端连接状态、代理映射和隧道多路复用器。
package manager

import (
	"BBgrid/common/model"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// Config 客户端管理器配置
type Config struct {
	ClientToken    string                                  // 客户端注册令牌
	PublicIP       string                                  // 服务器公网 IP
	PingInterval   time.Duration                           // 心跳间隔
	OnClientReady  func(clientID string, conn *Connection) // 客户端注册完成回调
	OnRelayMessage func(clientID string, msg interface{})  // 中继消息回调
}

// ClientManager 管理所有客户端连接
//
// 使用 sync.Map 实现并发安全的客户端存储， 并发安全map
// portIndex 提供端口到客户端的快速查找。
type ClientManager struct {
	clients         sync.Map       // clientID -> *ClientTable
	pendingRequests sync.Map       // requestID -> chan *PortsListData
	pendingClose    sync.Map       // key -> chan struct{} (proxy close ack)
	config          Config         // 管理器配置
	portIndex       map[int]string // port -> clientID 索引
	portIdxMu       sync.RWMutex   // 端口索引锁
	stopChan        chan struct{}
}

// NewClientManager 创建客户端管理器
func NewClientManager(cfg Config) *ClientManager {
	cfg.PingInterval = 30 * time.Second
	return &ClientManager{
		config:    cfg,
		portIndex: make(map[int]string),
		stopChan:  make(chan struct{}),
	}
}

// OnClientReady 触发客户端注册完成回调
func (m *ClientManager) OnClientReady(clientID string, conn *Connection) {
	if m.config.OnClientReady != nil {
		m.config.OnClientReady(clientID, conn)
	}
}

// SetOnClientReady 设置客户端注册完成回调
func (m *ClientManager) SetOnClientReady(callback func(string, *Connection)) {
	m.config.OnClientReady = callback
}

// SetOnRelayMessage 设置中继消息回调
func (m *ClientManager) SetOnRelayMessage(callback func(string, interface{})) {
	m.config.OnRelayMessage = callback
}

// DispatchRelayMessage 分发中继消息
func (m *ClientManager) DispatchRelayMessage(clientID string, msg interface{}) {
	if m.config.OnRelayMessage != nil {
		m.config.OnRelayMessage(clientID, msg)
	}
}

// Add 注册客户端
func (m *ClientManager) Add(clientID string, table *ClientTable) {
	m.clients.Store(clientID, table)
}

// Remove 移除客户端并清理相关资源。
func (m *ClientManager) Remove(clientID string) {
	table, ok := m.Get(clientID)
	if !ok {
		return
	}
	table.Cleanup()
	m.clients.Delete(clientID)

	m.portIdxMu.Lock()
	for port, cid := range m.portIndex {
		if cid == clientID {
			delete(m.portIndex, port)
		}
	}
	m.portIdxMu.Unlock()
}

// Get 获取客户端表。
func (m *ClientManager) Get(clientID string) (*ClientTable, bool) {
	val, ok := m.clients.Load(clientID)
	if !ok {
		return nil, false
	}
	table, ok := val.(*ClientTable)
	if !ok {
		return nil, false
	}
	return table, true
}

// ListClients 返回所有已连接客户端的信息列表。
func (m *ClientManager) ListClients() []ClientInfo {
	var list []ClientInfo
	m.clients.Range(func(key, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		conn := table.Conn()
		lat := conn.Latency()
		online := time.Since(conn.LastPingAt()) < 2*m.config.PingInterval
		latencyStr := "未知"
		if lat > 0 {
			latencyStr = lat.Truncate(time.Millisecond).String()
		}
		list = append(list, ClientInfo{
			ID:          table.ClientID(),
			RemoteAddr:  table.RemoteAddr(),
			ConnectedAt: table.ConnectedAt(),
			ProxyCount:  table.ProxyCount(),
			Host:        table.Host(),
			Latency:     latencyStr,
			Online:      online,
		})
		return true
	})
	return list
}

// SendCommand 向指定客户端发送命令。
func (m *ClientManager) SendCommand(clientID string, cmd interface{}) error {
	table, ok := m.Get(clientID)
	if !ok {
		return ErrClientNotFound
	}
	return table.Conn().WriteJSON(cmd)
}

// ErrClientNotFound 客户端未找到错误。
var ErrClientNotFound = errors.New("client not found")

// ClientInfo 客户端信息。
type ClientInfo struct {
	ID          string `json:"id"`
	RemoteAddr  string `json:"remote_addr"`
	ConnectedAt int64  `json:"connected_at"`
	ProxyCount  int    `json:"proxy_count"`
	Host        string `json:"host"`
	Latency     string `json:"latency"`
	Online      bool   `json:"online"`
}

// RegisterPendingRequest 注册待处理的请求。
func (m *ClientManager) RegisterPendingRequest(requestID string, ch chan *model.PortsListData) {
	m.pendingRequests.Store(requestID, ch)
}

// UnregisterPendingRequest 注销待处理的请求。
func (m *ClientManager) UnregisterPendingRequest(requestID string) {
	m.pendingRequests.Delete(requestID)
}

// RegisterPendingClose 注册待处理的关闭请求。
func (m *ClientManager) RegisterPendingClose(key string, ch chan struct{}) {
	m.pendingClose.Store(key, ch)
}

// UnregisterPendingClose 注销待处理的关闭请求。
func (m *ClientManager) UnregisterPendingClose(key string) {
	m.pendingClose.Delete(key)
}

// GetPendingClose 获取待处理的关闭请求通道。
func (m *ClientManager) GetPendingClose(key string) (chan struct{}, bool) {
	val, ok := m.pendingClose.Load(key)
	if !ok {
		return nil, false
	}
	ch, ok := val.(chan struct{})
	if !ok {
		return nil, false
	}
	return ch, true
}

// GetPendingRequest 获取待处理的请求通道。
func (m *ClientManager) GetPendingRequest(requestID string) (chan *model.PortsListData, bool) {
	val, ok := m.pendingRequests.Load(requestID)
	if !ok {
		return nil, false
	}
	ch, ok := val.(chan *model.PortsListData)
	if !ok {
		return nil, false
	}
	return ch, true
}

// GetPublicIP 返回服务器公网 IP。
func (m *ClientManager) GetPublicIP() string {
	return m.config.PublicIP
}

// 端口索引，用于快速查找代理

// RegisterPort 注册端口到客户端的映射。
func (m *ClientManager) RegisterPort(clientID string, port int) {
	m.portIdxMu.Lock()
	m.portIndex[port] = clientID
	m.portIdxMu.Unlock()
}

// UnregisterPort 注销端口映射。
func (m *ClientManager) UnregisterPort(port int) {
	m.portIdxMu.Lock()
	delete(m.portIndex, port)
	m.portIdxMu.Unlock()
}

// GetClientIDByPort 根据端口获取客户端 ID。
func (m *ClientManager) GetClientIDByPort(port int) (string, bool) {
	m.portIdxMu.RLock()
	defer m.portIdxMu.RUnlock()
	clientID, ok := m.portIndex[port]
	return clientID, ok
}

// FindTableByWSToken 查找拥有给定 WS 隧道令牌的 ClientTable。
func (m *ClientManager) FindTableByWSToken(token string) (*ClientTable, string, error) {
	var found *ClientTable
	var foundKey string
	m.clients.Range(func(_, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		key, err := table.GetWSToken(token)
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

// ListAllProxies 返回所有客户端的代理信息。
func (m *ClientManager) ListAllProxies() []map[string]interface{} {
	publicIP := m.config.PublicIP
	var result []map[string]interface{}
	m.clients.Range(func(_, value interface{}) bool {
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		for _, p := range table.ListProxies() {
			var portStr string
			switch v := p.Listener.(type) {
			case net.Listener:
				_, portStr, _ = net.SplitHostPort(v.Addr().String())
			}
			if portStr == "" {
				portStr = strconv.Itoa(p.RemotePort)
			}
			result = append(result, map[string]interface{}{
				"remote_port": p.RemotePort,
				"local_port":  p.LocalPort,
				"public_addr": publicIP + ":" + portStr,
				"client_id":   table.ClientID(),
			})
		}
		return true
	})
	return result
}

// RangeClients 遍历所有客户端
func (m *ClientManager) RangeClients(fn func(clientID string, table *ClientTable) bool) {
	m.clients.Range(func(key, value interface{}) bool {
		clientID, ok := key.(string)
		if !ok {
			return true
		}
		table, ok := value.(*ClientTable)
		if !ok {
			return true
		}
		return fn(clientID, table)
	})
}

// StopHealthCheck 停止健康检查
func (m *ClientManager) StopHealthCheck() {
	select {
	case <-m.stopChan:
	default:
		close(m.stopChan)
	}
}

func (m *ClientManager) StartHealthCheck() {
	go func() {
		ticker := time.NewTicker(m.config.PingInterval)
		defer ticker.Stop()

		sendPing := func() {
			m.clients.Range(func(key, value any) bool {
				table, ok := value.(*ClientTable)
				if !ok {
					return true
				}
				conn := table.Conn()
				now := time.Now()
				err := conn.WriteJSON(&model.WSMessage{
					Type: "ping",
					Data: now.Format(time.RFC3339Nano),
				})
				if err == nil {
					conn.mu.Lock()
					conn.lastPingAt = now
					conn.mu.Unlock()
				}
				return true
			})
		}

		sendPing()
		for {
			select {
			case <-m.stopChan:
				return
			case <-ticker.C:
				sendPing()
			}
		}
	}()
}
