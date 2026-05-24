package manager

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket 心跳和消息大小常量
const (
	writeWait      = 10 * time.Second // 写超时
	pongWait       = 40 * time.Second // 等待 pong 超时
	pingPeriod     = 30 * time.Second // 发送 ping 间隔
	maxMessageSize = 65536            // 最大消息大小
)

// Connection 封装 WebSocket 连接
//
// 提供并发安全的读写、心跳检测和自动重连支持。
// 使用 writePump/readPump 模式处理消息收发。
type Connection struct {
	wsConn      *websocket.Conn
	clientID    string
	table       *ClientTable
	host        string
	remoteIP    string
	send        chan []byte   // 发送缓冲区
	done        chan struct{} // 关闭信号
	manager     *ClientManager
	registered  bool // 是否已注册
	mu          sync.RWMutex
	closeOnce   sync.Once
	connectedAt time.Time
	lastPingAt  time.Time
	latency     time.Duration
}

// NewConnection 创建新的 WebSocket 连接封装
func NewConnection(wsConn *websocket.Conn, mgr *ClientManager) *Connection {
	return &Connection{
		wsConn:      wsConn,
		manager:     mgr,
		send:        make(chan []byte, 256),
		done:        make(chan struct{}),
		connectedAt: time.Now(),
		lastPingAt:  time.Now(),
	}
}

// Start 启动读写协程和注册超时检测
func (c *Connection) Start() {
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
//
// 负责：
// - 设置读限制和 pong 处理器
// - 读取并解析消息
// - 分发到对应处理器
func (c *Connection) readPump() {
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
				alog.Error(alog.CatMux, "WebSocket读取错误", "error", err)
			}
			break
		}

		var wsMsg model.WSMessage
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			alog.Error(alog.CatMux, "JSON反序列化错误", "error", err)
			continue
		}

		c.handleMessage(&wsMsg)
	}
}

func (c *Connection) handleMessage(msg *model.WSMessage) {
	switch msg.Type {
	case "register":
		c.handleRegister(msg.Data)
	case "response":
		alog.Debug(alog.CatClient, "客户端响应", "clientID", c.clientID, "data", msg.Data)
	case "ports_list":
		var portsData model.PortsListData
		b, err := json.Marshal(msg.Data)
		if err != nil {
			alog.Error(alog.CatMux, "ports_list序列化错误", "error", err)
			return
		}
		if err := json.Unmarshal(b, &portsData); err != nil {
			alog.Error(alog.CatMux, "ports_list反序列化错误", "error", err)
			return
		}

		if ch, ok := c.manager.GetPendingRequest(portsData.RequestID); ok {
			ch <- &portsData
			c.manager.UnregisterPendingRequest(portsData.RequestID)
		}
	case "relay_established", "relay_closed":
		c.manager.DispatchRelayMessage(c.clientID, msg)
	case "proxy_close_ack":
		key, _ := msg.Data.(string)
		if key != "" {
			if ch, ok := c.manager.GetPendingClose(key); ok {
				close(ch)
				c.manager.UnregisterPendingClose(key)
			}
		}
	case "pong":
		c.handlePong(msg.Data)
	default:
		alog.Warn(alog.CatMux, "未知消息类型", "type", msg.Type)
	}
}

func (c *Connection) getRemoteIP() string {
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

func (c *Connection) handleRegister(data interface{}) {
	var reg model.RegisterData

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

	if subtle.ConstantTimeCompare([]byte(reg.Token), []byte(c.manager.config.ClientToken)) != 1 {
		c.sendError(401, "invalid token")
		c.Close()
		return
	}

	if reg.ClientID == "" {
		c.sendError(400, "client_id required")
		return
	}

	if old, exists := c.manager.Get(reg.ClientID); exists {
		old.Conn().Close()
	}

	c.mu.Lock()
	c.clientID = reg.ClientID
	c.registered = true
	c.remoteIP = c.getRemoteIP()
	c.mu.Unlock()

	table := NewClientTable(reg.ClientID, c, c.remoteIP, c.connectedAt.Unix(), c.host)
	c.table = table
	c.manager.Add(reg.ClientID, table)

	serverHost := c.GetHost()
	if serverHost == "" {
		serverHost = c.remoteIP
	}
	respData, err := json.Marshal(map[string]string{
		"client_id":   reg.ClientID,
		"server_host": serverHost,
	})
	if err != nil {
		alog.Error(alog.CatClient, "序列化注册响应错误", "error", err)
		return
	}
	resp := model.WSMessage{
		Type: "registered",
		Data: string(respData),
	}
	if err := c.WriteJSON(&resp); err != nil {
		alog.Error(alog.CatClient, "写入注册响应错误", "error", err)
		return
	}

	alog.Info(alog.CatClient, "客户端已注册", "clientID", reg.ClientID, "serverHost", serverHost)

	// 触发客户端注册完成回调
	c.manager.OnClientReady(reg.ClientID, c)
}

func (c *Connection) sendError(code int, message string) {
	errData, err := json.Marshal(model.ErrorData{Code: code, Message: message})
	if err != nil {
		alog.Error(alog.CatClient, "序列化错误响应失败", "error", err)
		return
	}
	errMsg := model.WSMessage{
		Type: "error",
		Data: string(errData),
	}
	if err := c.WriteJSON(&errMsg); err != nil {
		alog.Error(alog.CatClient, "写入错误响应失败", "error", err)
	}
}

// writePump 写入 WebSocket 消息
//
// 负责：
// - 从 send 通道取消息并发送
// - 定期发送 ping 保持连接
func (c *Connection) writePump() {
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
			c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// WriteJSON 将 v 序列化为 JSON 并通过发送通道写入（带超时）。
func (c *Connection) WriteJSON(v interface{}) (err error) {
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

// Close 安全地关闭连接，确保清理操作只执行一次。
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		clientID := c.clientID
		c.registered = false
		c.mu.Unlock()

		if clientID != "" {
			c.manager.Remove(clientID)
		}
		close(c.send)
		c.wsConn.Close()
	})
	return nil
}

// IsRegistered 返回客户端是否已注册。
func (c *Connection) IsRegistered() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.registered
}

// GetInfo 返回客户端信息。
func (c *Connection) GetInfo() ClientInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return ClientInfo{
		ID:          c.clientID,
		RemoteAddr:  c.wsConn.RemoteAddr().String(),
		ConnectedAt: c.connectedAt.Unix(),
	}
}

// GetRemoteAddr 返回远端地址。
func (c *Connection) GetRemoteAddr() string {
	return c.wsConn.RemoteAddr().String()
}

// SetHost 设置服务器主机地址。
func (c *Connection) SetHost(host string) {
	c.mu.Lock()
	c.host = host
	c.mu.Unlock()
}

// GetHost 返回服务器主机地址。
func (c *Connection) GetHost() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.host
}

// GetTunnelHost 返回隧道主机地址（优先使用 Host，否则使用公网 IP）。
func (c *Connection) GetTunnelHost() string {
	host := c.GetHost()
	if host == "" {
		host = c.manager.GetPublicIP()
	}
	return host
}

// GetRemoteIP 返回客户端远端 IP。
func (c *Connection) GetRemoteIP() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.remoteIP
}

// Table 返回客户端表。
func (c *Connection) Table() *ClientTable {
	return c.table
}

func (c *Connection) handlePong(data interface{}) {
	ts, ok := data.(string)
	if !ok {
		alog.Debug(alog.CatClient, "pong data is not string", "clientID", c.clientID, "data", data)
		return
	}

	sentAt, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		alog.Debug(alog.CatClient, "pong timestamp parse failed", "clientID", c.clientID, "data", ts)
		return
	}
	c.mu.Lock()
	c.latency = time.Since(sentAt)
	c.mu.Unlock()
}

func (c *Connection) Latency() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.latency
}

func (c *Connection) LastPingAt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastPingAt
}
