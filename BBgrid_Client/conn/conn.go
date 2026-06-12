// Package conn 提供 WebSocket 连接封装，支持心跳检测和并发安全的消息传递。
//
// 遵循服务端 manager/connection.go 的 readPump/writePump 模式。
package conn

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 40 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 65536
)

// MessageHandler 是接收到 WSMessage 时的回调函数。
type MessageHandler func(msg *model.WSMessage)

// Connection 封装 WebSocket 连接，支持心跳检测和并发安全的写入（通过缓冲发送通道）。
type Connection struct {
	wsConn    *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
	onMessage MessageHandler
}

// New 创建新的 Connection，封装给定的 WebSocket 连接。
func New(wsConn *websocket.Conn, onMessage MessageHandler) *Connection {
	return &Connection{
		wsConn:    wsConn,
		send:      make(chan []byte, 256),
		done:      make(chan struct{}),
		onMessage: onMessage,
	}
}

// Start 启动读写协程。
func (c *Connection) Start() {
	go c.readPump()
	go c.writePump()
}

// Done 返回一个通道，当连接终止时关闭。
func (c *Connection) Done() <-chan struct{} {
	return c.done
}

// readPump 从 WebSocket 读取消息并分发给处理器。
// 管理读取截止时间和 pong 心跳响应。
func (c *Connection) readPump() {
	defer c.Close()

	c.wsConn.SetReadLimit(maxMessageSize)
	c.wsConn.SetPongHandler(func(string) error {
		return c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_ = c.wsConn.SetReadDeadline(time.Now().Add(pongWait))
		_, msg, err := c.wsConn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				alog.Error(alog.CatClient, "ws read error", "err", err)
			}
			break
		}

		var wsMsg model.WSMessage
		if err := json.Unmarshal(msg, &wsMsg); err != nil {
			alog.Error(alog.CatClient, "ws unmarshal error", "err", err)
			continue
		}

		if c.onMessage != nil {
			c.onMessage(&wsMsg)
		}
	}
}

// writePump 发送排队的消息和周期性 ping 以保持连接活跃。
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Close()
	}()

	for {
		select {
		case msg := <-c.send:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.wsConn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

// WriteJSON 将 v 序列化为 JSON 并排队通过写泵发送。
func (c *Connection) WriteJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	select {
	case c.send <- data:
		return nil
	case <-c.done:
		return fmt.Errorf("connection closed")
	}
}

// Close 安全地终止连接，确保清理操作只执行一次。
func (c *Connection) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		c.wsConn.Close()
	})
}
