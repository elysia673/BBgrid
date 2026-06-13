// Package client 提供核心客户端抽象，实现无感调用。
package client

import (
	"BBgrid/BBgrid_Client/auth"
	"BBgrid/BBgrid_Client/transport"
	"BBgrid/common/model"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
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
	CACertPath     string        `json:"ca_cert_path"`
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
	certCh         chan string // 证书接收通道
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
		certCh:        make(chan string, 1),
		eventHandlers: make(map[Event][]EventHandler),
		msgHandlers:   make([]MessageHandler, 0),
	}
}

// Connect 连接到服务器
func (c *clientImpl) Connect(ctx context.Context) error {
	c.mu.RLock()
	currentState := c.state
	c.mu.RUnlock()

	if currentState == StateConnected || currentState == StateRegistered {
		return nil
	}

	c.mu.Lock()
	c.state = StateConnecting
	c.mu.Unlock()

	log.Printf("[Client] Connecting to %s...", c.config.ServerURL)

	authMgr := auth.NewManager(auth.Config{
		Mode:           auth.AuthMode(c.getAuthMode()),
		PrivateKeyPath: c.config.PrivateKeyPath,
		CertPath:       c.config.CertPath,
		CACertPath:     c.config.CACertPath,
		Token:          c.config.ClientToken,
		Insecure:       c.config.Insecure,
	})

	if err := authMgr.Init(); err != nil {
		c.mu.Lock()
		c.state = StateIdle
		c.mu.Unlock()
		return fmt.Errorf("auth init: %w", err)
	}

	// 获取 TLS 配置
	var tlsConfig *tls.Config
	if authMgr.GetTLSConfig() != nil {
		tlsConfig = authMgr.GetTLSConfig()
		log.Printf("[Client] Using mTLS config: certs=%v", len(tlsConfig.Certificates) > 0)
	} else {
		log.Printf("[Client] No TLS config from authMgr, using default (token mode)")
	}

	trans := transport.NewWSTransport(c.config.ServerURL,
		transport.WithHTTP(c.config.UseHTTP),
		transport.WithInsecure(c.config.Insecure),
		transport.WithSNIOverride(c.config.TLSSNI),
		transport.WithOrigin(c.config.Origin),
		transport.WithOnMessage(c.handleMessage),
		transport.WithTLSConfig(tlsConfig),
	)

	if err := trans.Connect(ctx); err != nil {
		c.mu.Lock()
		c.state = StateIdle
		c.mu.Unlock()
		return fmt.Errorf("connect: %w", err)
	}

	c.mu.Lock()
	c.transport = trans
	c.auth = authMgr
	c.state = StateConnected
	c.mu.Unlock()

	log.Printf("[Client] Connected to server")
	c.emit(EventConnect, nil)

	log.Printf("[Client] Registering as %s...", c.config.ClientID)
	if err := c.register(); err != nil {
		if err == ErrRegistrationPending {
			// 保持连接，启动消息循环等待证书
			log.Printf("[Client] Waiting for certificate...")
			if wsTransport, ok := interface{}(trans).(*transport.WSTransport); ok {
				wsTransport.StartMessageLoop(ctx)
			}
			return err
		}
		trans.Close()
		c.mu.Lock()
		c.transport = nil
		c.state = StateIdle
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	c.state = StateRegistered
	c.mu.Unlock()

	log.Printf("[Client] Registered successfully")
	c.emit(EventRegister, nil)

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
	c.state = StateIdle
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

		err := c.Connect(ctx)
		if err == ErrCertificateReceived {
			// 收到证书，立即用 mTLS 重连
			log.Printf("[Client] Certificate received, reconnecting with mTLS...")
			c.Disconnect()
			continue
		} else if err == ErrRegistrationPending {
			// 注册等待审核，保持连接等待证书
			log.Printf("[Client] Registration pending, waiting for certificate...")
			select {
			case <-ctx.Done():
				return nil
			case <-c.stopCh:
				return nil
			case <-c.certCh:
				// 收到证书，重新连接
				log.Printf("[Client] Certificate received, reconnecting...")
				c.Disconnect()
				continue
			case <-c.waitForDisconnect():
				log.Printf("[Client] Disconnected, reconnecting...")
			}
		} else if err != nil {
			// 其他连接失败
			log.Printf("[Client] Connection failed: %v, retrying in %v...", err, c.config.ReconnectDelay)
			c.emit(EventError, err)
		} else {
			// 连接成功，等待断开或证书
			select {
			case <-ctx.Done():
				return nil
			case <-c.stopCh:
				return nil
			case <-c.certCh:
				// 收到证书，重新连接
				log.Printf("[Client] Certificate received, reconnecting...")
				c.Disconnect()
				continue
			case <-c.waitForDisconnect():
				log.Printf("[Client] Disconnected, reconnecting in %v...", c.config.ReconnectDelay)
				c.Disconnect()
			}
		}

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
	publicKeyPath := c.config.PublicKeyPath
	c.mu.RUnlock()

	publicKey := ""
	if data, err := os.ReadFile(publicKeyPath); err == nil {
		publicKey = string(data)
	}

	regMsg := model.WSMessage{
		Type: "register",
		Data: model.RegisterData{
			ClientID:  clientID,
			Token:     clientToken,
			PublicKey: publicKey,
		},
	}

	log.Printf("[Client] Sending register message...")
	if err := trans.Send(regMsg); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

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

	switch wsMsg.Type {
	case "registered":
		return nil
	case "register_pending":
		log.Printf("[Client] Registration submitted, waiting for approval")
		return ErrRegistrationPending
	case "certificate":
		// 服务器直接发了证书（已批准的客户端）
		log.Printf("[Client] Certificate received from server, saving and reconnecting with mTLS...")
		c.handleCertificateMessage(wsMsg.Data)
		// 清空 certCh 中的残留值，避免下次 ErrRegistrationPending 立即触发
		select {
		case <-c.certCh:
		default:
		}
		return ErrCertificateReceived
	default:
		return fmt.Errorf("registration failed: %v", wsMsg)
	}
}

// handleMessage 处理消息
func (c *clientImpl) handleMessage(msg *model.WSMessage) {
	if msg.Type == "certificate" {
		c.handleCertificateMessage(msg.Data)
		return
	}

	c.emit(EventMessage, msg)

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

// handleCertificateMessage 处理证书消息
func (c *clientImpl) handleCertificateMessage(data any) {
	m, ok := data.(map[string]any)
	if !ok {
		log.Printf("[Client] Invalid certificate message data")
		return
	}

	certificate, _ := m["certificate"].(string)
	caCert, _ := m["ca_cert"].(string)

	if certificate == "" {
		log.Printf("[Client] Empty certificate")
		return
	}

	// 保存客户端证书
	if err := os.WriteFile(c.config.CertPath, []byte(certificate), 0600); err != nil {
		log.Printf("[Client] Save certificate failed: %v", err)
		return
	}
	log.Printf("[Client] Certificate saved to %s", c.config.CertPath)

	// 保存 CA 证书
	if caCert != "" && c.config.CACertPath != "" {
		if err := os.WriteFile(c.config.CACertPath, []byte(caCert), 0600); err != nil {
			log.Printf("[Client] Save CA cert failed: %v", err)
		} else {
			log.Printf("[Client] CA cert saved to %s", c.config.CACertPath)
		}
	}

	// 通知 Run 循环重新连接
	select {
	case c.certCh <- certificate:
	default:
		log.Printf("[Client] Certificate channel full, dropping")
	}
}

// emit 触发事件
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
	mode := string(auth.AuthModeToken)
	if c.config.CertPath != "" && c.config.PrivateKeyPath != "" {
		certExists := fileExists(c.config.CertPath)
		keyExists := fileExists(c.config.PrivateKeyPath)
		if certExists && keyExists {
			mode = string(auth.AuthModeMTLS)
		}
		log.Printf("[Client] getAuthMode: certPath=%s certExists=%v keyPath=%s keyExists=%v => %s",
			c.config.CertPath, certExists, c.config.PrivateKeyPath, keyExists, mode)
	}
	return mode
}

// fileExists 检查文件是否存在
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ErrRegistrationPending 注册等待审核错误
var ErrRegistrationPending = fmt.Errorf("registration pending: waiting for approval")

// ErrCertificateReceived 收到证书，需要重连
var ErrCertificateReceived = fmt.Errorf("certificate received, reconnecting with mTLS")
