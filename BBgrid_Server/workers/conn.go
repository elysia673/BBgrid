package workers

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket 常量
const (
	writeWait      = 10 * time.Second
	pongWait       = 40 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 65536
)

// clientConn WebSocket 连接封装
//
// 实现 ClientConn 接口，提供并发安全的读写。
type clientConn struct {
	wsConn      *websocket.Conn
	clientID    string
	host        string
	remoteIP    string
	send        chan []byte
	done        chan struct{}
	state       StateStore
	dispatcher  Dispatcher
	auth        *AuthWorker
	registered  bool
	isTemp      bool
	mu          sync.RWMutex
	closeOnce   sync.Once
	connectedAt time.Time
	lastPingAt  time.Time
	latency     time.Duration
}

// newClientConn 创建新的 WebSocket 连接封装
func newClientConn(wsConn *websocket.Conn, state StateStore, dispatcher Dispatcher, auth *AuthWorker) *clientConn {
	return &clientConn{
		wsConn:      wsConn,
		state:       state,
		dispatcher:  dispatcher,
		auth:        auth,
		send:        make(chan []byte, 256),
		done:        make(chan struct{}),
		connectedAt: time.Now(),
		lastPingAt:  time.Now(),
	}
}

// Start 启动读写协程和注册超时检测
func (c *clientConn) Start() {
	go c.writePump()
	go c.readPump()

	go func() {
		time.Sleep(30 * time.Second)
		if !c.IsRegistered() {
			alog.Warn(alog.CatClient, "客户端注册超时，关闭连接")
			c.Close()
		}
	}()
}

// readPump 读取 WebSocket 消息
func (c *clientConn) readPump() {
	defer c.Close()

	c.wsConn.SetReadLimit(maxMessageSize)
	if err := c.wsConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		alog.Error(alog.CatMux, "设置初始读截止时间错误", "error", err)
	}
	c.wsConn.SetPongHandler(func(string) error {
		return c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, msg, err := c.wsConn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				alog.Error(alog.CatMux, "WebSocket 读取错误", "error", err)
			}
			break
		}

		// 收到任何有效消息都重置读取截止时间（兼容 JSON 心跳）
		_ = c.wsConn.SetReadDeadline(time.Now().Add(pongWait))

		var wsMsg struct {
			Type string      `json:"type"`
			Data interface{} `json:"data,omitempty"`
		}
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			alog.Error(alog.CatMux, "JSON 反序列化错误", "error", err)
			continue
		}

		c.handleMessage(&wsMsg)
	}
}

// handleMessage 处理消息
func (c *clientConn) handleMessage(msg *struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}) {
	switch msg.Type {
	case "register":
		c.handleRegister(msg.Data)
	case "response":
		alog.Debug(alog.CatClient, "客户端响应", "clientID", c.clientID, "data", msg.Data)
	case "pong":
		c.handlePong(msg.Data)
	default:
		alog.Warn(alog.CatMux, "未知消息类型", "type", msg.Type)
	}
}

// handleRegister 处理注册消息
func (c *clientConn) handleRegister(data interface{}) {
	var reg struct {
		ClientID string `json:"client_id"`
		Token    string `json:"token"`
	}

	switch v := data.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &reg); err != nil {
			c.sendError(400, "invalid register data")
			return
		}
	default:
		b, err := json.Marshal(data)
		if err != nil {
			c.sendError(400, "invalid register data")
			return
		}
		if err := json.Unmarshal(b, &reg); err != nil {
			c.sendError(400, "invalid register data")
			return
		}
	}

	// 验证 token
	if !c.auth.ValidateClientToken(reg.Token) {
		c.sendError(401, "invalid token")
		c.Close()
		return
	}

	if reg.ClientID == "" {
		c.sendError(400, "client_id required")
		return
	}

	// 如果已存在，关闭旧连接
	if old, ok := c.state.GetClient(reg.ClientID); ok {
		old.Conn().Close()
	}

	c.mu.Lock()
	c.clientID = reg.ClientID
	c.registered = true
	c.remoteIP = c.getRemoteIP()
	c.mu.Unlock()

	// 设置远程地址
	remoteAddr := c.wsConn.RemoteAddr().String()

	// 添加到状态机（StateWorker.AddClient 会自动发布 client/ADDED 事件）
	c.state.AddClient(reg.ClientID, c, remoteAddr)

	// 发送注册成功响应
	serverHost := c.GetHost()
	if serverHost == "" {
		serverHost = c.remoteIP
	}
	respData, _ := json.Marshal(map[string]string{
		"client_id":   reg.ClientID,
		"server_host": serverHost,
	})
	resp := map[string]interface{}{
		"type": "registered",
		"data": string(respData),
	}
	if err := c.WriteJSON(resp); err != nil {
		alog.Error(alog.CatClient, "写入注册响应错误", "error", err)
		return
	}

	alog.Info(alog.CatClient, "客户端已注册", "clientID", reg.ClientID, "serverHost", serverHost)
}

// handlePong 处理 pong 消息
func (c *clientConn) handlePong(data interface{}) {
	ts, ok := data.(string)
	if !ok {
		return
	}
	sentAt, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return
	}

	latency := time.Since(sentAt)

	c.mu.Lock()
	c.latency = latency
	clientID := c.clientID
	c.mu.Unlock()

	// 发布延迟事件到 Dispatcher
	if clientID != "" && c.dispatcher != nil {
		event := proto.NewGenericEvent(
			proto.ResourceKey{
				Type:      proto.ResourceTypeClient,
				Namespace: proto.NamespaceDefault,
				Name:      clientID,
			},
			proto.EventModified,
			map[string]any{"latency": latency},
		)
		c.dispatcher.Dispatch(event)
	}
}

// sendError 发送错误消息
func (c *clientConn) sendError(code int, message string) {
	errData, _ := json.Marshal(map[string]interface{}{
		"code":    code,
		"message": message,
	})
	errMsg := map[string]interface{}{
		"type": "error",
		"data": string(errData),
	}
	c.WriteJSON(errMsg)
}

// writePump 写入 WebSocket 消息
func (c *clientConn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.wsConn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			// 发送 JSON 格式的 ping 消息
			c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			pingMsg := map[string]any{
				"type": "ping",
				"data": time.Now().Format(time.RFC3339Nano),
			}
			data, _ := json.Marshal(pingMsg)
			if err := c.wsConn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// WriteJSON 将 v 序列化为 JSON 并通过发送通道写入
func (c *clientConn) WriteJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
	case <-c.done:
		return fmt.Errorf("connection closed")
	case <-time.After(3 * time.Second):
		return fmt.Errorf("write timeout")
	}
	return nil
}

// Close 安全地关闭连接
func (c *clientConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		clientID := c.clientID
		isTemp := c.isTemp
		c.registered = false
		c.mu.Unlock()

		if clientID != "" {
			// RemoveClient 会自动发布 client/DELETED 事件
			c.state.RemoveClient(clientID)
			if isTemp && c.auth != nil {
				c.auth.RemoveTempClient(clientID)
			}
		}
		c.wsConn.Close()
	})
	return nil
}

// ==================== ClientConn 接口实现 ====================

// GetHost 返回服务器主机地址
func (c *clientConn) GetHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.host
}

// SetHost 设置服务器主机地址
func (c *clientConn) SetHost(host string) {
	c.mu.Lock()
	c.host = host
	c.mu.Unlock()
}

// GetRemoteIP 返回客户端远端 IP
func (c *clientConn) GetRemoteIP() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.remoteIP
}

// IsTemp 检查是否为临时节点
func (c *clientConn) IsTemp() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.isTemp
}

// SetTemp 设置是否为临时节点
func (c *clientConn) SetTemp(isTemp bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isTemp = isTemp
}

// IsRegistered 返回客户端是否已注册
func (c *clientConn) IsRegistered() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.registered
}

// SetRegistered 设置注册状态
func (c *clientConn) SetRegistered(registered bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registered = registered
}

// SetClientID 设置客户端 ID
func (c *clientConn) SetClientID(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientID = clientID
}

// Latency 返回延迟
func (c *clientConn) Latency() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latency
}

// LastPingAt 返回最后心跳时间
func (c *clientConn) LastPingAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastPingAt
}

// getRemoteIP 获取远端 IP
func (c *clientConn) getRemoteIP() string {
	addr := c.wsConn.RemoteAddr()
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return tcpAddr.IP.String()
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}
