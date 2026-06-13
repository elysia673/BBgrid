// Package daemon 提供与 daemon 通信的客户端库。
package daemon

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Message 消息协议
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// RegisterPayload 注册消息
type RegisterPayload struct {
	Name    string `json:"name"`
	PID     int    `json:"pid"`
	Version string `json:"version"`
}

// HeartbeatPayload 心跳消息
type HeartbeatPayload struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Client daemon 客户端
type Client struct {
	name       string
	version    string
	socketPath string
	conn       net.Conn
	mu         sync.Mutex
	stopCh     chan struct{}
	connected  bool
}

// New 创建客户端
func New(name, version, socketPath string) *Client {
	if socketPath == "" {
		socketPath = getDefaultSocketPath()
	}
	return &Client{
		name:       name,
		version:    version,
		socketPath: socketPath,
		stopCh:     make(chan struct{}),
	}
}

// getDefaultSocketPath 获取默认 socket 路径
func getDefaultSocketPath() string {
	base := getBaseDir()
	return filepath.Join(base, "data", "daemon.sock")
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	execPath, err := os.Executable()
	if err != nil {
		return "/usr/local/bbgrid"
	}
	return filepath.Dir(filepath.Dir(execPath))
}

// Connect 连接 daemon
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected && c.conn != nil {
		return nil
	}

	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("connect daemon: %w", err)
	}

	c.conn = conn
	c.connected = true
	return nil
}

// Close 关闭连接
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	close(c.stopCh)

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.connected = false
}

// Register 注册到 daemon
func (c *Client) Register() error {
	if err := c.Connect(); err != nil {
		return err
	}

	payload := RegisterPayload{
		Name:    c.name,
		PID:     os.Getpid(),
		Version: c.version,
	}

	msg := Message{
		Type:    "register",
		Payload: mustMarshal(payload),
	}

	if err := c.send(msg); err != nil {
		return err
	}

	var resp Message
	if err := c.receive(&resp); err != nil {
		return err
	}

	if resp.Type == "error" {
		var errMsg string
		json.Unmarshal(resp.Payload, &errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	return nil
}

// StartHeartbeat 启动心跳
func (c *Client) StartHeartbeat() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.sendHeartbeat()
			}
		}
	}()
}

// sendHeartbeat 发送心跳
func (c *Client) sendHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected || c.conn == nil {
		// 尝试重连
		conn, err := net.Dial("unix", c.socketPath)
		if err != nil {
			return
		}
		c.conn = conn
		c.connected = true
	}

	payload := HeartbeatPayload{
		Name:   c.name,
		Status: "running",
	}

	msg := Message{
		Type:    "heartbeat",
		Payload: mustMarshal(payload),
	}

	c.send(msg)
}

// send 发送消息
func (c *Client) send(msg Message) error {
	if c.conn == nil {
		return fmt.Errorf("not connected")
	}
	return json.NewEncoder(c.conn).Encode(msg)
}

// receive 接收消息
func (c *Client) receive(msg *Message) error {
	if c.conn == nil {
		return fmt.Errorf("not connected")
	}
	return json.NewDecoder(c.conn).Decode(msg)
}

// mustMarshal 序列化 JSON
func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
