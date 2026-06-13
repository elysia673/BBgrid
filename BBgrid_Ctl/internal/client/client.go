// Package client 提供与 daemon 通信的客户端。
package client

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"os"
)

// Message 消息协议
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// ServiceStatus 服务状态
type ServiceStatus struct {
	Running bool   `json:"running"`
	PID     int    `json:"pid"`
	Version string `json:"version"`
}

// Client daemon 客户端
type Client struct {
	socketPath string
	conn       net.Conn
}

// New 创建客户端
func New(socketPath string) *Client {
	if socketPath == "" {
		socketPath = getDefaultSocketPath()
	}
	return &Client{
		socketPath: socketPath,
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
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("connect daemon: %w", err)
	}
	c.conn = conn
	return nil
}

// Close 关闭连接
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// SendCommand 发送命令
func (c *Client) SendCommand(command, target string) error {
	if err := c.Connect(); err != nil {
		return err
	}
	defer c.Close()

	payload := map[string]any{
		"command": command,
		"target":  target,
	}

	msg := Message{
		Type:    "command",
		Payload: mustMarshal(payload),
	}

	if err := json.NewEncoder(c.conn).Encode(msg); err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	var resp Message
	if err := json.NewDecoder(c.conn).Decode(&resp); err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.Type == "error" {
		var errMsg string
		json.Unmarshal(resp.Payload, &errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	return nil
}

// GetStatus 获取状态
func (c *Client) GetStatus() (map[string]ServiceStatus, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}
	defer c.Close()

	msg := Message{Type: "status"}
	if err := json.NewEncoder(c.conn).Encode(msg); err != nil {
		return nil, fmt.Errorf("send status: %w", err)
	}

	var resp Message
	if err := json.NewDecoder(c.conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.Type == "error" {
		var errMsg string
		json.Unmarshal(resp.Payload, &errMsg)
		return nil, fmt.Errorf("%s", errMsg)
	}

	var status map[string]ServiceStatus
	json.Unmarshal(resp.Payload, &status)
	return status, nil
}

// Ping 检查 daemon 是否运行
func (c *Client) Ping() error {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("daemon not running")
	}
	conn.Close()
	return nil
}

func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
