package main

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// TempClient 临时客户端
type TempClient struct {
	wsURL    string
	clientID string
	token    string
	insecure bool
	conn     *websocket.Conn
	stopCh   chan struct{}
	handler  *TempHandler
}

// NewTempClient 创建临时客户端
func NewTempClient(wsURL, clientID, token string, insecure bool) *TempClient {
	return &TempClient{
		wsURL:    wsURL,
		clientID: clientID,
		token:    token,
		insecure: insecure,
		stopCh:   make(chan struct{}),
		handler:  NewTempHandler(),
	}
}

// Run 运行临时客户端
func (c *TempClient) Run() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		err := c.connect()
		if err != nil {
			alog.Error(alog.CatClient, "connection error", "error", err)
		}

		select {
		case <-c.stopCh:
			return
		case <-time.After(5 * time.Second):
			alog.Info(alog.CatClient, "reconnecting...")
		}
	}
}

// Stop 停止客户端
func (c *TempClient) Stop() {
	close(c.stopCh)
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *TempClient) connect() error {
	u, err := url.Parse(c.wsURL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}

	dialer := websocket.DefaultDialer
	if c.insecure {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	c.conn = conn
	defer func() {
		conn.Close()
		c.conn = nil
	}()

	// 设置pong handler
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	alog.Info(alog.CatClient, "connected", "url", c.wsURL)

	// 发送临时节点注册消息
	regMsg := model.WSMessage{
		Type: "temp_register",
		Data: map[string]string{
			"client_id": c.clientID,
			"token":     c.token,
		},
	}
	if err := conn.WriteJSON(&regMsg); err != nil {
		return fmt.Errorf("register failed: %w", err)
	}

	// 等待注册确认
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var resp model.WSMessage
	if err := conn.ReadJSON(&resp); err != nil {
		return fmt.Errorf("read register response: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	if resp.Type != "temp_registered" {
		return fmt.Errorf("unexpected response: %s", resp.Type)
	}

	// 解析响应
	data, _ := json.Marshal(resp.Data)
	var regResp struct {
		ClientID   string `json:"client_id"`
		ServerHost string `json:"server_host"`
	}
	json.Unmarshal(data, &regResp)

	alog.Info(alog.CatClient, "registered as temp node", "client_id", regResp.ClientID, "server", regResp.ServerHost)

	// 设置消息处理器
	c.handler.SetSender(conn)

	// 设置初始read deadline（心跳由服务器ping驱动）
	conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	// 消息循环
	for {
		select {
		case <-c.stopCh:
			return nil
		default:
		}

		var msg model.WSMessage
		if err := conn.ReadJSON(&msg); err != nil {
			alog.Error(alog.CatClient, "read error", "error", err)
			return err
		}

		// 收到任何消息都更新read deadline
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))

		c.handler.Handle(&msg)
	}
}
