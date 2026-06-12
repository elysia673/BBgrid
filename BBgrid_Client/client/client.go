// Package client 提供核心客户端抽象，实现无感调用。
package client

import (
	"BBgrid/BBgrid_Client/auth"
	"BBgrid/BBgrid_Client/transport"
	"BBgrid/common/model"
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// State 客户端状态
type State string

const (
	StateIdle       State = "idle"
	StateConnecting State = "connecting"
	StateConnected  State = "connected"
	StateRegistered State = "registered"
	StateClosed     State = "closed"
)

// Event 客户端事件
type Event string

const (
	EventConnect    Event = "connect"
	EventDisconnect Event = "disconnect"
	EventRegister   Event = "register"
	EventError      Event = "error"
	EventMessage    Event = "message"
)

// EventHandler 事件处理器
type EventHandler func(event Event, data any)

// MessageHandler 消息处理器
type MessageHandler func(msg *model.WSMessage)

// Client 客户端接口
type Client interface {
	Connect(ctx context.Context) error
	Disconnect() error
	IsConnected() bool
	GetState() State
	GetID() string
	Send(msg any) error
	On(event Event, handler EventHandler)
	Off(event Event)
	OnMessage(handler MessageHandler)
	Run(ctx context.Context) error
	Stop()
}

// Config 客户端配置
type Config struct {
	ServerURL      string        `json:"server_url"`
	ClientID       string        `json:"client_id"`
	ClientToken    string        `json:"client_token"`
	Voucher        string        `json:"voucher"`
	PrivateKeyPath string        `json:"private_key_path"`
	PublicKeyPath  string        `json:"public_key_path"`
	CertPath       string        `json:"cert_path"`
	DataDir        string        `json:"data_dir"`
	LogPath        string        `json:"log_path"`
	UseHTTP        bool          `json:"use_http"`
	Insecure       bool          `json:"insecure"`
	TLSSNI         string        `json:"tls_sni"`
	Origin         string        `json:"origin"`
	UDPTunnelKey   string        `json:"udp_tunnel_key"`
	ReconnectDelay time.Duration `json:"reconnect_delay"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		ReconnectDelay: 5 * time.Second,
	}
}

// clientImpl 客户端实现
type clientImpl struct {
	mu             sync.RWMutex
	config         Config
	state          State
	transport      transport.Transport
	auth           *auth.Manager
	stopCh         chan struct{}
	eventHandlers  map[Event][]EventHandler
	msgHandlers    []MessageHandler
	msgHandlersMu  sync.RWMutex
}

// New 创建新客户端
func New(config Config) Client {
	return &clientImpl{
		config:        config,
		state:         StateIdle,
		stopCh:        make(chan struct{}),
		eventHandlers: make(map[Event][]EventHandler),
		msgHandlers:   make([]MessageHandler, 0),
	}
}

// Connect 连接到服务器
func (c *clientImpl) Connect(ctx context.Context) error {
	// 检查状态 - 只有 Idle 状态才需要连接
	c.mu.RLock()
	currentState := c.state
	c.mu.RUnlock()

	if currentState == StateConnected || currentState == StateRegistered {
		return nil
	}

	// 设置状态
	c.mu.Lock()
	c.state = StateConnecting
	c.mu.Unlock()

	log.Printf("[Client] Connecting to %s...", c.config.ServerURL)

	// 创建认证管理器
	authMgr := auth.NewManager(auth.Config{
		Mode:           auth.AuthMode(c.getAuthMode()),
		PrivateKeyPath: c.config.PrivateKeyPath,
		PublicKeyPath:  c.config.PublicKeyPath,
		CertPath:       c.config.CertPath,
		Token:          c.config.ClientToken,
		Insecure:       c.config.Insecure,
	})

	if err := authMgr.Init(); err != nil {
		c.mu.Lock()
		c.state = StateIdle
		c.mu.Unlock()
		return fmt.Errorf("auth init: %w", err)
	}

	// 创建传输层
	trans := transport.NewWSTransport(c.config.ServerURL,
		transport.WithHTTP(c.config.UseHTTP),
		transport.WithInsecure(c.config.Insecure),
		transport.WithSNIOverride(c.config.TLSSNI),
		transport.WithOrigin(c.config.Origin),
		transport.WithOnMessage(c.handleMessage),
	)

	// 连接
	if err := trans.Connect(ctx); err != nil {
		c.mu.Lock()
		c.state = StateIdle
		c.mu.Unlock()
		return fmt.Errorf("connect: %w", err)
	}

	// 保存 transport 和 auth
	c.mu.Lock()
	c.transport = trans
	c.auth = authMgr
	c.state = StateConnected
	c.mu.Unlock()

	log.Printf("[Client] Connected to server")
	c.emit(EventConnect, nil)

	// 注册
	log.Printf("[Client] Registering as %s...", c.config.ClientID)
	if err := c.register(); err != nil {
		trans.Close()
		c.mu.Lock()
		c.transport = nil
		c.state = StateIdle
		c.mu.Unlock()
		return fmt.Errorf("register: %w", err)
	}

	c.mu.Lock()
	c.state = StateRegistered
	c.mu.Unlock()

	log.Printf("[Client] Registered successfully")
	c.emit(EventRegister, nil)

	// 启动消息循环
	if wsTransport, ok := interface{}(trans).(*transport.WSTransport); ok {
		wsTransport.StartMessageLoop(ctx)
	}

	return nil
}

// Disconnect 断开连接
func (c *clientImpl) Disconnect() error {
	c.mu.Lock()
	trans := c.transport
	c.transport = nil
	c.state = StateIdle // 重置为 Idle 而不是 Closed
	c.mu.Unlock()

	if trans != nil {
		log.Printf("[Client] Disconnecting...")
		if err := trans.Close(); err != nil {
			return err
		}
	}

	c.emit(EventDisconnect, nil)
	return nil
}

// IsConnected 是否已连接
func (c *clientImpl) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transport != nil && c.transport.IsConnected()
}

// GetState 获取客户端状态
func (c *clientImpl) GetState() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// GetID 获取客户端 ID
func (c *clientImpl) GetID() string {
	return c.config.ClientID
}

// Send 发送消息
func (c *clientImpl) Send(msg any) error {
	c.mu.RLock()
	trans := c.transport
	c.mu.RUnlock()

	if trans == nil || !trans.IsConnected() {
		return fmt.Errorf("not connected")
	}

	return trans.Send(msg)
}

// On 注册事件处理器
func (c *clientImpl) On(event Event, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventHandlers[event] = append(c.eventHandlers[event], handler)
}

// Off 注销事件处理器
func (c *clientImpl) Off(event Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.eventHandlers, event)
}

// OnMessage 注册消息处理器
func (c *clientImpl) OnMessage(handler MessageHandler) {
	c.msgHandlersMu.Lock()
	defer c.msgHandlersMu.Unlock()
	c.msgHandlers = append(c.msgHandlers, handler)
}

// Run 运行客户端主循环
func (c *clientImpl) Run(ctx context.Context) error {
	log.Printf("[Client] Starting client loop...")
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stopCh:
			return nil
		default:
		}

		if err := c.Connect(ctx); err != nil {
			log.Printf("[Client] Connection failed: %v, retrying in %v...", err, c.config.ReconnectDelay)
			c.emit(EventError, err)
			time.Sleep(c.config.ReconnectDelay)
			continue
		}

		// 等待断开连接
		<-c.waitForDisconnect()
		log.Printf("[Client] Disconnected, reconnecting in %v...", c.config.ReconnectDelay)

		// 确保状态重置
		c.Disconnect()

		select {
		case <-c.stopCh:
			return nil
		case <-time.After(c.config.ReconnectDelay):
			continue
		}
	}
}

// Stop 停止客户端
func (c *clientImpl) Stop() {
	select {
	case <-c.stopCh:
		// already closed
	default:
		close(c.stopCh)
	}
	c.Disconnect()
}

// register 注册到服务器
func (c *clientImpl) register() error {
	c.mu.RLock()
	trans := c.transport
	clientID := c.config.ClientID
	clientToken := c.config.ClientToken
	c.mu.RUnlock()

	regMsg := model.WSMessage{
		Type: "register",
		Data: model.RegisterData{
			ClientID: clientID,
			Token:    clientToken,
		},
	}

	log.Printf("[Client] Sending register message...")
	if err := trans.Send(regMsg); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	// 等待注册响应
	log.Printf("[Client] Waiting for register response...")
	msg, err := trans.Receive()
	if err != nil {
		return fmt.Errorf("receive register response: %w", err)
	}

	wsMsg, ok := msg.(*model.WSMessage)
	if !ok {
		return fmt.Errorf("invalid message type")
	}

	log.Printf("[Client] Received response: type=%s", wsMsg.Type)

	if wsMsg.Type != "registered" {
		return fmt.Errorf("registration failed: %v", wsMsg)
	}

	return nil
}

// handleMessage 处理消息
func (c *clientImpl) handleMessage(msg *model.WSMessage) {
	// 触发消息事件
	c.emit(EventMessage, msg)

	// 分发给所有消息处理器
	c.msgHandlersMu.RLock()
	handlers := make([]MessageHandler, len(c.msgHandlers))
	copy(handlers, c.msgHandlers)
	c.msgHandlersMu.RUnlock()

	for _, handler := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[Client] Message handler panic: %v", r)
				}
			}()
			handler(msg)
		}()
	}
}

// emit 触发事件（异步调用处理器）
func (c *clientImpl) emit(event Event, data any) {
	c.mu.RLock()
	handlers := c.eventHandlers[event]
	c.mu.RUnlock()

	for _, handler := range handlers {
		go handler(event, data)
	}
}

// waitForDisconnect 等待断开连接
func (c *clientImpl) waitForDisconnect() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for {
			if !c.IsConnected() {
				close(ch)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()
	return ch
}

// getAuthMode 获取认证模式
func (c *clientImpl) getAuthMode() string {
	if c.config.CertPath != "" && c.config.PrivateKeyPath != "" {
		return string(auth.AuthModeMTLS)
	}
	return string(auth.AuthModeToken)
}
