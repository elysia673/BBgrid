package workers

import (
	alog "BBgrid/common/log"
	"BBgrid/common/wsconn"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ==================== 接口定义 ====================

// WSHandler WebSocket 处理器接口
//
// 定义 WebSocket 连接管理的核心能力，未来可以用 gRPC 实现。
type WSHandler interface {
	// Handle 处理常驻节点 WebSocket 连接
	Handle(w http.ResponseWriter, r *http.Request)

	// HandleTemp 处理临时节点 WebSocket 连接
	HandleTemp(w http.ResponseWriter, r *http.Request)

	// HandleTunnel 处理 WebSocket 隧道
	HandleTunnel(w http.ResponseWriter, r *http.Request)
}

// ==================== WebSocket 实现 ====================

// WSConfig WebSocket 配置
type WSConfig struct {
	Domain      string
	ClientToken string
}

// WSWorker WebSocket Worker
//
// 实现 WSHandler 接口，处理 WebSocket 连接。
type WSWorker struct {
	config     WSConfig
	state      StateStore
	auth       *AuthWorker
	dispatcher Dispatcher
	stopCh     chan struct{}
}

// NewWSWorker 创建 WebSocket Worker
func NewWSWorker(config WSConfig, state StateStore, auth *AuthWorker) *WSWorker {
	return &WSWorker{
		config: config,
		state:  state,
		auth:   auth,
		stopCh: make(chan struct{}),
	}
}

// Name 返回 Worker 名称
func (w *WSWorker) Name() string {
	return "ws"
}

// SetDispatcher 设置事件分发器
func (w *WSWorker) SetDispatcher(d Dispatcher) {
	w.dispatcher = d
}

// Run 启动 WebSocket Worker
func (w *WSWorker) Run() error {
	alog.Info(alog.CatSystem, "WSWorker 启动")

	// 等待停止信号
	<-w.stopCh
	alog.Info(alog.CatSystem, "WSWorker 停止")
	return nil
}

// Stop 停止 WebSocket Worker
func (w *WSWorker) Stop() {
	close(w.stopCh)
}

// ==================== WSHandler 接口实现 ====================

// Handle 处理常驻节点 WebSocket 连接
func (w *WSWorker) Handle(rw http.ResponseWriter, r *http.Request) {
	// 验证客户端证书
	certs := r.TLS.PeerCertificates
	if len(certs) == 0 {
		http.Error(rw, `{"error":"client certificate required"}`, http.StatusUnauthorized)
		return
	}

	// 从证书中提取客户端 ID
	clientCert := certs[0]
	clientID := clientCert.Subject.CommonName

	// 验证证书是否在注册表中且状态为 approved
	record := w.auth.GetByClientID(clientID)
	if record == nil || record.Status != "approved" {
		alog.Warn(alog.CatAuth, "客户端证书验证失败", "clientID", clientID)
		http.Error(rw, `{"error":"client certificate revoked or not approved"}`, http.StatusUnauthorized)
		return
	}

	// 升级 WebSocket 连接
	conn, err := upgrader(w.config.Domain).Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatClient, "WebSocket 升级错误", "error", err)
		return
	}

	// 提取请求 Host
	host := r.Host
	if hostHost, _, err := net.SplitHostPort(host); err == nil {
		host = hostHost
	}

	// 创建连接并启动
	clientConn := newClientConn(conn, w.state, w.dispatcher, w.auth)
	clientConn.SetHost(host)
	clientConn.Start()
}

// HandleTemp 处理临时节点 WebSocket 连接
func (w *WSWorker) HandleTemp(rw http.ResponseWriter, r *http.Request) {
	// 升级 WebSocket 连接
	conn, err := upgrader(w.config.Domain).Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatClient, "WebSocket 升级错误", "error", err)
		return
	}

	// 读取注册消息
	var regMsg struct {
		Type string            `json:"type"`
		Data map[string]string `json:"data"`
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := conn.ReadJSON(&regMsg); err != nil {
		alog.Error(alog.CatAuth, "读取临时注册消息失败", "error", err)
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	if regMsg.Type != "temp_register" {
		alog.Error(alog.CatAuth, "无效的临时注册消息类型", "type", regMsg.Type)
		conn.Close()
		return
	}

	clientID := regMsg.Data["client_id"]
	token := regMsg.Data["token"]

	// 验证 token
	if token != w.config.ClientToken {
		alog.Warn(alog.CatAuth, "无效的临时客户端 token", "clientID", clientID)
		conn.WriteJSON(map[string]interface{}{
			"type": "error",
			"data": map[string]string{"message": "invalid token"},
		})
		conn.Close()
		return
	}

	// 注册临时客户端到注册表
	if err := w.auth.AddTempClient(clientID); err != nil {
		alog.Error(alog.CatAuth, "注册临时客户端失败", "error", err)
	}

	// 发送注册成功响应
	host := r.Host
	if hostHost, _, err := net.SplitHostPort(host); err == nil {
		host = hostHost
	}

	regResp := map[string]interface{}{
		"type": "temp_registered",
		"data": map[string]string{
			"client_id":   clientID,
			"server_host": host,
		},
	}
	if err := conn.WriteJSON(regResp); err != nil {
		alog.Error(alog.CatClient, "发送临时注册响应失败", "error", err)
		conn.Close()
		return
	}

	alog.Info(alog.CatClient, "临时客户端已注册", "clientID", clientID)

	// 创建连接并启动
	clientConn := newClientConn(conn, w.state, w.dispatcher, w.auth)
	clientConn.SetHost(host)
	clientConn.SetClientID(clientID)
	clientConn.SetTemp(true)
	clientConn.SetRegistered(true)
	clientConn.Start()
}

// HandleRelay 处理中继 WebSocket 连接
func (w *WSWorker) HandleRelay(rw http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	token := r.URL.Query().Get("token")
	role := r.URL.Query().Get("role")
	clientID := r.URL.Query().Get("client_id")

	if sessionID == "" || token == "" || role == "" || clientID == "" {
		http.Error(rw, `{"error":"missing params"}`, http.StatusBadRequest)
		return
	}

	// 验证会话
	session, ok := w.state.GetRelaySession(sessionID)
	if !ok {
		http.Error(rw, `{"error":"session not found"}`, http.StatusNotFound)
		return
	}
	if session.Token != token {
		http.Error(rw, `{"error":"invalid token"}`, http.StatusForbidden)
		return
	}
	if role == "source" && clientID != session.SourceClient {
		http.Error(rw, `{"error":"client not authorized"}`, http.StatusForbidden)
		return
	}
	if role == "target" && clientID != session.TargetClient {
		http.Error(rw, `{"error":"client not authorized"}`, http.StatusForbidden)
		return
	}

	// 升级 WebSocket 连接
	conn, err := upgrader(w.config.Domain).Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatRelay, "中继 WebSocket 升级错误", "error", err)
		return
	}

	alog.Info(alog.CatRelay, "中继已连接", "role", role, "sessionID", sessionID)

	// TODO: 实现中继桥接
	// 需要等待双方都连接后，启动双向转发
	conn.Close()
}

// HandleTunnel 处理 WebSocket 隧道
func (w *WSWorker) HandleTunnel(rw http.ResponseWriter, r *http.Request) {
	// 升级 WebSocket 连接
	conn, err := upgrader(w.config.Domain).Upgrade(rw, r, nil)
	if err != nil {
		alog.Error(alog.CatTunnel, "WebSocket 升级错误", "error", err)
		return
	}

	// 读取认证消息
	_, msg, err := conn.ReadMessage()
	if err != nil {
		alog.Error(alog.CatAuth, "读取隧道认证失败", "error", err)
		conn.Close()
		return
	}

	var authMsg struct {
		Type string `json:"type"`
		Data struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &authMsg); err != nil {
		alog.Error(alog.CatAuth, "解析隧道认证失败", "error", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid auth"})
		conn.Close()
		return
	}

	if authMsg.Type != "tunnel_auth" {
		alog.Warn(alog.CatAuth, "隧道认证类型错误", "type", authMsg.Type)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "unexpected message type"})
		conn.Close()
		return
	}

	// 查找对应的客户端
	clientState, key, err := w.state.FindTableByWSToken(authMsg.Data.Token)
	if err != nil {
		alog.Error(alog.CatAuth, "隧道令牌无效", "error", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid token"})
		conn.Close()
		return
	}

	// 创建隧道连接
	wsAdapter := wsconn.New(conn)
	tc := newTunnelConn(wsAdapter)

	// 发送就绪消息
	ready := map[string]any{
		"type": "tunnel_ready",
		"data": map[string]string{"status": "ok"},
	}
	if err := conn.WriteJSON(ready); err != nil {
		alog.Error(alog.CatTunnel, "发送就绪消息失败", "error", err)
		tc.Close()
		return
	}

	// 保存隧道连接
	clientState.PutTunnel(key, tc)
	alog.Info(alog.CatTunnel, "隧道已创建", "key", key)

	// 等待关闭
	<-tc.Done()
	clientState.RemoveTunnel(key, tc)
	alog.Info(alog.CatTunnel, "隧道已关闭", "key", key)
}

// ==================== 辅助函数 ====================

// upgrader 创建 WebSocket upgrader
func upgrader(domain string) *websocket.Upgrader {
	return &websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true
			}
			if domain == "" {
				return true
			}
			if strings.HasPrefix(origin, "https://"+domain) {
				return true
			}
			if strings.HasPrefix(origin, "http://"+domain) {
				return true
			}
			alog.Warn(alog.CatClient, "WebSocket 连接被拒绝", "origin", origin)
			return false
		},
		HandshakeTimeout: 10 * time.Second,
	}
}

// tunnelConn 隧道连接包装器
//
// 包装 net.Conn 并提供生命周期管理（done channel）。
// 注意：这不是 common/mux 中的多路复用器，只是单连接的生命周期包装。
type tunnelConn struct {
	conn     net.Conn
	done     chan struct{}
	closeOne sync.Once
}

// newTunnelConn 创建隧道连接
func newTunnelConn(conn net.Conn) *tunnelConn {
	tc := &tunnelConn{
		conn: conn,
		done: make(chan struct{}),
	}
	return tc
}

func (tc *tunnelConn) Close() {
	tc.closeOne.Do(func() {
		close(tc.done)
		tc.conn.Close()
	})
}

func (tc *tunnelConn) Done() <-chan struct{} {
	return tc.done
}

func (tc *tunnelConn) GetConn() net.Conn {
	return tc.conn
}
