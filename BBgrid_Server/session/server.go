package session

import (
	"BBgrid/BBgrid_Server/auth"
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/proto"
	"BBgrid/common/util"
	"BBgrid/common/wsconn"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ==================== Session ====================

// Session 客户端会话
type Session struct {
	mu           sync.RWMutex // 保护下面的可变字段
	clientID     string
	conn         *websocket.Conn
	send         chan []byte
	done         chan struct{}
	closeOnce    sync.Once
	registered   chan struct{}
	registerOnce sync.Once
	isTemp       bool
	remoteAddr   string
	host         string
	connectedAt  time.Time
	lastPingAt   time.Time
	latency      time.Duration
	core         *runtime.Core
	auth         *auth.Manager
}

// NewSession 创建会话
func NewSession(conn *websocket.Conn, clientID string, isTemp bool, core *runtime.Core, authManager *auth.Manager) *Session {
	return &Session{
		clientID:    clientID,
		conn:        conn,
		send:        make(chan []byte, 256),
		done:        make(chan struct{}),
		registered:  make(chan struct{}),
		isTemp:      isTemp,
		remoteAddr:  conn.RemoteAddr().String(),
		connectedAt: time.Now(),
		core:        core,
		auth:        authManager,
	}
}

// ClientID 获取客户端 ID
func (s *Session) ClientID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clientID
}

// Start 启动会话 (readPump + writePump)
func (s *Session) Start() {
	go s.writePump()
	go s.readPump()
}

// readPump 读取 WS 消息
func (s *Session) readPump() {
	defer s.Close()

	s.conn.SetReadLimit(65536)
	s.conn.SetPongHandler(func(string) error {
		s.conn.SetReadDeadline(time.Now().Add(40 * time.Second))
		return nil
	})

	for {
		s.conn.SetReadDeadline(time.Now().Add(40 * time.Second))
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				alog.Warn(alog.CatWS, "WS 异常关闭", "client_id", s.clientID, "error", err)
			}
			return
		}

		var msg model.WSMessage
		if err := json.Unmarshal(message, &msg); err != nil {
			continue
		}

		s.handleMessage(msg)
	}
}

// writePump 写入 WS 消息
func (s *Session) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		// 注意：不在这里关闭连接，由 Close() 方法统一处理
	}()

	for {
		select {
		case message, ok := <-s.send:
			s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				s.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := s.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			pingMsg := model.WSMessage{Type: "ping"}
			data, _ := json.Marshal(pingMsg)
			if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}

		case <-s.done:
			return
		}
	}
}

// handleMessage 处理 WS 消息
func (s *Session) handleMessage(msg model.WSMessage) {
	switch msg.Type {
	case "register":
		s.handleRegister(msg.Data)
	case "temp_register":
		s.handleTempRegister(msg.Data)
	case "pong":
		s.handlePong(msg.Data)
	case "response":
		// 客户端响应，忽略
	default:
		alog.Debug(alog.CatWS, "未知 WS 消息类型", "type", msg.Type, "client_id", s.clientID)
	}
}

// handleRegister 处理注册消息
func (s *Session) handleRegister(data interface{}) {
	regData, ok := data.(map[string]any)
	if !ok {
		return
	}

	token, _ := regData["token"].(string)
	clientID, _ := regData["client_id"].(string)

	if token == "" || clientID == "" {
		s.sendError(400, "missing token or client_id")
		return
	}

	// 验证 token
	if !s.auth.ValidateClientToken(token) {
		s.sendError(401, "invalid token")
		return
	}

	if !s.isTemp && s.clientID != "" && s.clientID != clientID {
		s.sendError(403, "client_id does not match certificate")
		return
	}

	s.mu.Lock()
	s.clientID = clientID
	s.host, _ = regData["host"].(string)
	s.mu.Unlock()

	// 发布 client ADDED 事件
	event := newClientEvent(s.clientID, s.remoteAddr, s.host)
	s.core.Publish(event)

	// 存储 ClientConn 到 StateStore (活连接)
	s.core.StateStore().SetClientConn(s.clientID, s)

	// 自动分配命名空间（如果还没有）
	ns := "permanent"
	if s.isTemp {
		ns = "temporary"
	}
	s.auth.SetClientNamespace(s.clientID, ns, "node")

	// 发送 registered 响应
	resp := model.WSMessage{
		Type: "registered",
		Data: model.RegisteredData{
			ClientID:   s.clientID,
			ServerHost: s.core.StateStore().GetPublicIP(),
		},
	}
	s.WriteJSON(resp)
	s.registerOnce.Do(func() { close(s.registered) })

	alog.Info(alog.CatWS, "客户端注册成功", "client_id", s.clientID)
}

// handleTempRegister 处理临时注册消息
func (s *Session) handleTempRegister(data interface{}) {
	regData, ok := data.(map[string]any)
	if !ok {
		return
	}

	token, _ := regData["token"].(string)
	clientID, _ := regData["client_id"].(string)

	if token == "" || clientID == "" {
		s.sendError(400, "missing token or client_id")
		return
	}

	// 验证 token
	if !s.auth.ValidateClientToken(token) {
		s.sendError(401, "invalid token")
		return
	}

	s.mu.Lock()
	s.clientID = clientID
	s.mu.Unlock()

	// 发布 client ADDED 事件
	event := newClientEvent(s.clientID, s.remoteAddr, "")
	s.core.Publish(event)

	// 存储 ClientConn 到 StateStore (活连接)
	s.core.StateStore().SetClientConn(s.clientID, s)

	// 临时客户端自动分配到 temporary 命名空间
	s.auth.SetClientNamespace(s.clientID, "temporary", "node")

	// 发送 registered 响应
	resp := model.WSMessage{
		Type: "registered",
		Data: model.RegisteredData{
			ClientID:   s.clientID,
			ServerHost: s.core.StateStore().GetPublicIP(),
		},
	}
	s.WriteJSON(resp)
	s.registerOnce.Do(func() { close(s.registered) })

	alog.Info(alog.CatWS, "临时客户端注册成功", "client_id", s.clientID)
}

// handlePong 处理 pong 消息 (计算延迟)
func (s *Session) handlePong(data interface{}) {
	pongData, ok := data.(map[string]any)
	if !ok {
		return
	}

	tsStr, ok := pongData["timestamp"].(string)
	if !ok {
		return
	}

	ts, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return
	}

	s.mu.Lock()
	s.latency = time.Since(ts)
	s.lastPingAt = time.Now()
	clientID := s.clientID
	s.mu.Unlock()

	// 刷新读取截止时间
	s.conn.SetReadDeadline(time.Now().Add(40 * time.Second))

	// 发布 client MODIFIED 事件 (延迟更新)
	event := newClientModifiedEvent(clientID, s.latency)
	s.core.Publish(event)
}

// WriteJSON 发送 JSON 消息
func (s *Session) WriteJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case s.send <- data:
		return nil
	case <-time.After(3 * time.Second):
		return fmt.Errorf("send timeout")
	}
}

// sendError 发送错误消息
func (s *Session) sendError(code int, message string) {
	s.WriteJSON(model.WSMessage{
		Type: "error",
		Data: model.ErrorData{
			Code:    code,
			Message: message,
		},
	})
}

// Close 关闭会话
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)

		// 发布 client DELETED 事件
		s.mu.RLock()
		clientID := s.clientID
		s.mu.RUnlock()
		if clientID != "" {
			event := newClientDeletedEvent(clientID)
			s.core.Publish(event)
		}

		alog.Info(alog.CatWS, "会话关闭", "client_id", clientID)
	})
	// conn.Close() 在 closeOnce 外调用，避免 writePump 还在写时 close 导致 panic。
	// writePump 通过 <-done 退出后再也不写 conn，此时关闭安全。
	s.conn.Close()
	return nil
}

// Done 返回关闭信号 channel
func (s *Session) Done() <-chan struct{} { return s.done }

// Registered 返回注册完成信号 channel
func (s *Session) Registered() <-chan struct{} { return s.registered }

// ==================== ClientConn 接口实现 ====================

// GetHost 获取主机名
func (s *Session) GetHost() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.host
}

// GetRemoteIP 获取远程 IP
func (s *Session) GetRemoteIP() string {
	host, _, _ := net.SplitHostPort(s.remoteAddr)
	return host
}

// IsTemp 是否临时连接
func (s *Session) IsTemp() bool { return s.isTemp }

// Latency 获取延迟
func (s *Session) Latency() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latency
}

// LastPingAt 获取最后 ping 时间
func (s *Session) LastPingAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastPingAt
}

// ==================== Server ====================

// pendingEntry 待配对的隧道连接
type pendingEntry struct {
	ch        chan net.Conn
	createdAt time.Time
}

// Server Session Layer 服务器
type Server struct {
	core               *runtime.Core
	auth               *auth.Manager
	tunnelPort         int
	sessions           sync.Map // clientID -> *Session
	upgrader           *websocket.Upgrader
	pendingMap         map[string]*pendingEntry
	pendingMu          sync.Mutex
	stopCh             chan struct{}
	stopOnce           sync.Once
	relayPending       map[string]chan *websocket.Conn // sessionID -> pending relay conn
	pendingRelaySignal map[string][]model.WSMessage     // clientID -> 待补发的 relay 信号
	relayMu            sync.Mutex
	activeLoops        map[string]bool // proxy key → tunnel loop 是否已启动
	loopsMu            sync.Mutex
}

// NewServer 创建 Session Layer 服务器
func NewServer(core *runtime.Core, domain string, tunnelPort int, authManager *auth.Manager) *Server {
	s := &Server{
		core:       core,
		auth:       authManager,
		tunnelPort: tunnelPort,
		upgrader: &websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		pendingMap:         make(map[string]*pendingEntry),
		stopCh:             make(chan struct{}),
		relayPending:       make(map[string]chan *websocket.Conn),
		pendingRelaySignal: make(map[string][]model.WSMessage),
		activeLoops:        make(map[string]bool),
	}

	// 注册 Proxy/Relay Provider（插件 Init 时完成，此处不再重复）
	// Session Handler 的 EventBus 订阅由 StartEventSubscriptions() 在插件初始化后调用

	// 启动清理 goroutine
	go s.cleanupLoop()

	return s
}

// StartEventSubscriptions 启动 EventBus 订阅（在插件初始化完成后调用）
func (s *Server) StartEventSubscriptions() {
	s.core.EventBus().Subscribe(proto.ResourceTypeProxy, s.handleProxyEvent)

	// relay 事件由 relay-provider 插件处理，session handler 不再订阅
	// s.core.EventBus().Subscribe(proto.ResourceTypeRelay, s.handleRelayEvent)

	// 注入 notifyFn 到 relay provider（用于通知客户端）
	if relayProvider, ok := s.core.Capability().GetRelayProvider("default"); ok {
		if rp, ok := relayProvider.(interface {
			SetNotifyFn(fn func(clientID string, msg any) error)
		}); ok {
			rp.SetNotifyFn(func(clientID string, msg any) error {
				return s.SendToClient(clientID, msg)
			})
		}
	}

	// 注入 notifyFn 到 proxy provider（用于通知客户端）
	if proxyProvider, ok := s.core.Capability().GetProxyProvider("tcp"); ok {
		if pp, ok := proxyProvider.(interface {
			SetNotifyFn(fn func(clientID string, msg any) error)
		}); ok {
			pp.SetNotifyFn(func(clientID string, msg any) error {
				return s.SendToClient(clientID, msg)
			})
		}
	}
}

// Stop 停止 Session Layer 服务器
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// Handle 处理永久节点 WS 连接 (mTLS)
func (s *Server) Handle(rw http.ResponseWriter, r *http.Request) {
	// 从 mTLS 证书提取 clientID
	clientID := extractClientID(r)
	if clientID == "" {
		alog.Warn(alog.CatWS, "WS 缺少客户端证书，降级为 register token 认证", "remote_addr", r.RemoteAddr)
	}

	conn, err := s.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatWS, "WS upgrade 失败", "error", err)
		return
	}

	session := NewSession(conn, clientID, false, s.core, s.auth)
	session.Start()

	s.trackSession(session, clientID)
}

// HandleTemp 处理临时节点 WS 连接
func (s *Server) HandleTemp(rw http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatWS, "WS upgrade 失败", "error", err)
		return
	}

	session := NewSession(conn, "", true, s.core, s.auth)
	session.Start()

	s.trackSession(session, "")
}

func (s *Server) trackSession(session *Session, initialClientID string) {
	select {
	case <-session.Registered():
	case <-session.Done():
		return
	case <-time.After(30 * time.Second):
		if session.ClientID() == "" {
			session.Close()
			return
		}
	}

	clientID := session.ClientID()
	if clientID != "" {
		s.sessions.Store(clientID, session)
		defer s.sessions.Delete(clientID)
		s.syncClientProxies(clientID)
		s.syncClientRelays(clientID)
		s.deliverPendingRelaySignals(clientID)
	}

	<-session.Done()
}

// syncClientRelays 客户端重连时，从 StateStore 查 relay session 并补发信号
func (s *Server) syncClientRelays(clientID string) {
	for _, relay := range s.core.StateStore().ListRelaySessions() {
		if relay.SourceClient != clientID && relay.TargetClient != clientID {
			continue
		}
		role := "source"
		peerClientID := relay.TargetClient
		if relay.TargetClient == clientID {
			role = "target"
			peerClientID = relay.SourceClient
		}

		msg := model.WSMessage{
			Type: "relay_signal",
			Data: map[string]any{
				"session_id":      relay.ID,
				"protocol":        relay.Protocol,
				"role":            role,
				"peer_client_id":  peerClientID,
				"source_port":     relay.SourcePort,
				"target_port":     relay.TargetPort,
				"target_local_ip": relay.TargetLocalIP,
				"source_local_ip": relay.SourceLocalIP,
				"server_host":     s.core.StateStore().GetPublicIP(),
				"token":           relay.Token,
			},
		}
		if err := s.SendToClient(clientID, msg); err != nil {
			alog.Warn(alog.CatWS, "补发 relay 信号失败", "client_id", clientID, "session_id", relay.ID, "error", err)
		} else {
			alog.Info(alog.CatWS, "补发 relay 信号成功", "client_id", clientID, "session_id", relay.ID, "role", role)
		}
	}
}

// deliverPendingRelaySignals 补发暂存的 relay 信号
func (s *Server) deliverPendingRelaySignals(clientID string) {
	s.pendingMu.Lock()
	signals := s.pendingRelaySignal[clientID]
	delete(s.pendingRelaySignal, clientID)
	s.pendingMu.Unlock()

	for _, msg := range signals {
		if err := s.SendToClient(clientID, msg); err != nil {
			alog.Warn(alog.CatWS, "补发 relay 信号失败", "client_id", clientID, "error", err)
		} else {
			alog.Info(alog.CatWS, "补发 relay 信号成功", "client_id", clientID)
		}
	}
}

func (s *Server) syncClientProxies(clientID string) {
	for _, proxy := range s.core.StateStore().ListProxies() {
		if proxy.ClientID != clientID {
			continue
		}
		msg := model.WSMessage{
			Type: "proxy",
			Data: model.CommandData{
				RemotePort: proxy.RemotePort,
				LocalPort:  proxy.LocalPort,
				LocalIP:    proxy.LocalIP,
				Protocol:   proxy.Protocol,
				BindAddr:   proxy.BindAddr,
				ServerHost: s.core.StateStore().GetPublicIP(),
				TunnelPort: s.tunnelPort,
			},
		}
		if err := s.SendToClient(clientID, msg); err != nil {
			alog.Warn(alog.CatWS, "同步客户端代理配置失败", "client_id", clientID, "port", proxy.RemotePort, "error", err)
		}

		// 启动隧道循环（客户端重连后需要重新建立隧道）
		proxyState := runtime.ProxyState{
			ClientID:   proxy.ClientID,
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
		}
		s.startTunnelLoop(proxyState)
	}
}

// SendToClient 发送消息给指定客户端
func (s *Server) SendToClient(clientID string, msg interface{}) error {
	val, ok := s.sessions.Load(clientID)
	if !ok {
		return fmt.Errorf("client not found: %s", clientID)
	}
	session := val.(*Session)
	return session.WriteJSON(msg)
}

// ==================== Event Handlers (Realtime Push) ====================

// handleProxyEvent 处理代理事件
//
// 当代理创建时：
// 1. 启动 TCP listener 在公网端口
// 2. 通知客户端准备接受隧道
func (s *Server) handleProxyEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		s.handleProxyAdded(event)
	case proto.EventDeleted:
		s.handleProxyDeleted(event)
	}
}

// handleProxyDeleted 处理代理删除事件
func (s *Server) handleProxyDeleted(event proto.GenericEvent) {
	key := event.Resource.Name

	// 通知客户端（listener 关闭由 plugin 处理）
	if clientID, remotePort, ok := parseProxyResourceKey(key); ok {
		msg := model.WSMessage{
			Type: "proxy_closed",
			Data: model.ProxyClosedData{Key: proxyClientKey(clientID, remotePort)},
		}
		if err := s.SendToClient(clientID, msg); err != nil {
			alog.Warn(alog.CatWS, "通知客户端关闭代理失败", "client_id", clientID, "key", key, "error", err)
		}
	}

	alog.Info(alog.CatSystem, "Proxy 已关闭", "key", key)
}

// handleProxyAdded 处理代理创建事件
func (s *Server) handleProxyAdded(event proto.GenericEvent) {
	proxy, ok := event.Payload.(runtime.ProxyState)
	if !ok {
		if m, ok := event.Payload.(map[string]any); ok {
			proxy = runtime.ProxyState{}
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
		} else {
			return
		}
	}

	if proxy.ClientID == "" || proxy.RemotePort == 0 {
		alog.Warn(alog.CatWS, "跳过无效 proxy ADDED 事件", "client_id", proxy.ClientID, "port", proxy.RemotePort)
		return
	}

	// 通知客户端（恢复期间跳过，客户端重连后 syncClientProxies 会补发）
	if !s.core.IsRestoring() {
		s.notifyProxyClient(proxy)
	}

	// 启动隧道接受循环（listener 已由 plugin 创建）
	s.startTunnelLoop(proxy)
}

// notifyProxyClient 通知客户端代理配置
func (s *Server) notifyProxyClient(proxy runtime.ProxyState) {
	msg := model.WSMessage{
		Type: "proxy",
		Data: model.CommandData{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			ServerHost: s.core.StateStore().GetPublicIP(),
			TunnelPort: s.tunnelPort,
		},
	}
	if err := s.SendToClient(proxy.ClientID, msg); err != nil {
		alog.Warn(alog.CatWS, "通知客户端代理配置失败",
			"client_id", proxy.ClientID, "port", proxy.RemotePort, "error", err)
	}
}

// startTunnelLoop 启动隧道接受循环（幂等：同一 proxy 不会重复启动）
func (s *Server) startTunnelLoop(proxy runtime.ProxyState) {
	key := fmt.Sprintf("%s:%d", proxy.ClientID, proxy.RemotePort)
	s.loopsMu.Lock()
	if s.activeLoops[key] {
		s.loopsMu.Unlock()
		return
	}
	s.activeLoops[key] = true
	s.loopsMu.Unlock()

	provider, ok := s.core.Capability().GetProxyProvider("tcp")
	if !ok {
		alog.Error(alog.CatSystem, "proxy provider 'tcp' 未注册")
		return
	}
	ln, ok := provider.GetListener(proxy.ClientID, proxy.RemotePort)
	if !ok {
		alog.Error(alog.CatSystem, "获取 proxy listener 失败", "port", proxy.RemotePort)
		return
	}

	clientKey := proxyClientKey(proxy.ClientID, proxy.RemotePort)

	go func() {
		defer func() {
			s.loopsMu.Lock()
			delete(s.activeLoops, key)
			s.loopsMu.Unlock()
		}()

		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				select {
				case <-s.stopCh:
					return
				default:
					alog.Error(alog.CatSystem, "Proxy accept 失败", "error", err)
					continue
				}
			}

			tunnelToken := proto.GenerateID()
			ch := s.registerPending(tunnelToken)

			msg := model.WSMessage{
				Type: "tunnel_request",
				Data: map[string]any{
					"key":        clientKey,
					"token":      tunnelToken,
					"local_port": proxy.LocalPort,
					"local_ip":   proxy.LocalIP,
					"protocol":   proxy.Protocol,
				},
			}

			if err := s.SendToClient(proxy.ClientID, msg); err != nil {
				alog.Error(alog.CatWS, "通知客户端建立隧道失败",
					"client_id", proxy.ClientID, "port", proxy.RemotePort, "error", err)
				conn.Close()
				continue
			}

			go func() {
				select {
				case tunnelConn := <-ch:
					if tunnelConn == nil {
						conn.Close()
						return
					}
					util.PipeBidir(conn, tunnelConn)
					conn.Close()
				case <-time.After(60 * time.Second):
					alog.Warn(alog.CatSystem, "隧道连接超时",
						"client_id", proxy.ClientID, "port", proxy.RemotePort)
					conn.Close()
				case <-s.stopCh:
					conn.Close()
				}
			}()
		}
	}()
}

// registerPending 注册待配对连接
func (s *Server) registerPending(token string) chan net.Conn {
	ch := make(chan net.Conn, 1)
	s.pendingMu.Lock()
	s.pendingMap[token] = &pendingEntry{
		ch:        ch,
		createdAt: time.Now(),
	}
	s.pendingMu.Unlock()
	return ch
}

func (s *Server) unregisterPending(token string) {
	s.pendingMu.Lock()
	delete(s.pendingMap, token)
	s.pendingMu.Unlock()
}

// AcceptTunnel 接受隧道连接
func (s *Server) AcceptTunnel(token string, conn net.Conn) {
	s.pendingMu.Lock()
	entry, ok := s.pendingMap[token]
	if ok {
		delete(s.pendingMap, token)
	}
	s.pendingMu.Unlock()

	if ok {
		// 发送 TUNL ACK，告知客户端隧道握手完成
		if _, err := conn.Write([]byte{0x01}); err != nil {
			conn.Close()
			return
		}
		entry.ch <- conn
	} else {
		conn.Close()
	}
}

// cleanupLoop 清理过期的 pending 连接
func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupExpired()
		case <-s.stopCh:
			return
		}
	}
}

// cleanupExpired 清理过期的 pending 连接
func (s *Server) cleanupExpired() {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	now := time.Now()
	for token, entry := range s.pendingMap {
		if now.Sub(entry.createdAt) > 60*time.Second {
			// 发送 nil 而非 close，避免发送方 panic
			entry.ch <- nil
			delete(s.pendingMap, token)
		}
	}
}

func proxyClientKey(clientID string, remotePort int) string {
	return fmt.Sprintf("%s-%d", clientID, remotePort)
}

func parseProxyResourceKey(key string) (string, int, bool) {
	clientID, portText, ok := strings.Cut(key, ":")
	if !ok || clientID == "" || portText == "" {
		return "", 0, false
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, false
	}
	return clientID, port, true
}

// extractClientID 从 mTLS 证书提取 clientID
func extractClientID(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
	return ""
}

// ==================== Tunnel WebSocket Handler ====================

// HandleTunnel 处理隧道 WebSocket 连接
func (s *Server) HandleTunnel(rw http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatWS, "Tunnel WS upgrade 失败", "error", err)
		return
	}
	defer conn.Close()

	// 读取认证消息
	var authMsg model.WSMessage
	if err := conn.ReadJSON(&authMsg); err != nil {
		alog.Error(alog.CatWS, "Tunnel 读取认证消息失败", "error", err)
		return
	}

	if authMsg.Type != "tunnel_auth" {
		conn.WriteJSON(model.WSMessage{Type: "error", Data: model.ErrorData{Code: 400, Message: "expected tunnel_auth"}})
		return
	}

	// 解析 token
	authData, ok := authMsg.Data.(map[string]any)
	if !ok {
		conn.WriteJSON(model.WSMessage{Type: "error", Data: model.ErrorData{Code: 400, Message: "invalid auth data"}})
		return
	}
	token, _ := authData["token"].(string)
	if token == "" {
		conn.WriteJSON(model.WSMessage{Type: "error", Data: model.ErrorData{Code: 400, Message: "missing token"}})
		return
	}

	// 接受隧道连接
	tunnelConn := wsconn.New(conn)
	s.AcceptTunnel(token, tunnelConn)

	// 发送 tunnel_ready 响应
	readyMsg := model.WSMessage{Type: "tunnel_ready"}
	if err := conn.WriteJSON(readyMsg); err != nil {
		alog.Error(alog.CatWS, "Tunnel 发送 ready 消息失败", "error", err)
		return
	}

	alog.Info(alog.CatWS, "Tunnel 连接已建立", "token", token[:8]+"...")

	// 等待连接关闭
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// ==================== Relay WebSocket Handler ====================

// HandleRelay 处理中继 WebSocket 连接——配对 source 和 target 两端做 PipeBidir
func (s *Server) HandleRelay(rw http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	token := r.URL.Query().Get("token")
	role := r.URL.Query().Get("role")
	clientID := r.URL.Query().Get("client_id")

	if sessionID == "" || token == "" || role == "" || clientID == "" {
		http.Error(rw, "missing required parameters", http.StatusBadRequest)
		return
	}

	relaySession, ok := s.core.StateStore().GetRelaySession(sessionID)
	if !ok {
		http.Error(rw, "session not found", http.StatusNotFound)
		return
	}
	if relaySession.Token != token {
		http.Error(rw, "invalid token", http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatWS, "Relay WS upgrade 失败", "error", err)
		return
	}

	alog.Info(alog.CatWS, "Relay 连接已建立", "session", sessionID, "role", role, "client_id", clientID)

	s.relayMu.Lock()
	ch, exists := s.relayPending[sessionID]
	if !exists {
		ch = make(chan *websocket.Conn, 1)
		s.relayPending[sessionID] = ch
		s.relayMu.Unlock()

		select {
		case peer := <-ch:
			if peer == nil {
				conn.Close()
				return
			}
			alog.Info(alog.CatWS, "Relay 配对成功 开始转发", "session", sessionID)
			relayWSPipe(conn, peer)
			alog.Info(alog.CatWS, "Relay 转发结束", "session", sessionID)
		case <-time.After(60 * time.Second):
			s.relayMu.Lock()
			delete(s.relayPending, sessionID)
			s.relayMu.Unlock()
			conn.Close()
		}
		return
	}
	delete(s.relayPending, sessionID)
	s.relayMu.Unlock()

	ch <- conn
}

func relayWSPipe(a, b *websocket.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			msgType, data, err := a.ReadMessage()
			if err != nil {
				alog.Info(alog.CatWS, "relay a→b read error", "error", err)
				return
			}
			alog.Debug(alog.CatWS, "relay a→b", "type", msgType, "size", len(data))
			if err := b.WriteMessage(msgType, data); err != nil {
				alog.Info(alog.CatWS, "relay a→b write error", "error", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			msgType, data, err := b.ReadMessage()
			if err != nil {
				alog.Info(alog.CatWS, "relay b→a read error", "error", err)
				return
			}
			alog.Debug(alog.CatWS, "relay b→a", "type", msgType, "size", len(data))
			if err := a.WriteMessage(msgType, data); err != nil {
				alog.Info(alog.CatWS, "relay b→a write error", "error", err)
				return
			}
		}
	}()
	wg.Wait()
	a.Close()
	b.Close()
}

// ==================== Event Helpers ====================

func newClientEvent(clientID, remoteAddr, host string) proto.GenericEvent {
	return proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeClient,
			Namespace: proto.NamespaceDefault,
			Name:      clientID,
		},
		proto.EventAdded,
		map[string]any{"remote_addr": remoteAddr, "host": host},
	)
}

func newClientModifiedEvent(clientID string, latency time.Duration) proto.GenericEvent {
	return proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeClient,
			Namespace: proto.NamespaceDefault,
			Name:      clientID,
		},
		proto.EventModified,
		map[string]any{"latency": latency.Milliseconds()},
	)
}

func newClientDeletedEvent(clientID string) proto.GenericEvent {
	return proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeClient,
			Namespace: proto.NamespaceDefault,
			Name:      clientID,
		},
		proto.EventDeleted,
		nil,
	)
}
