// Package transport 提供传输层抽象，支持多种传输协议。
package transport

import (
	"context"
	"net"
	"time"
)

// Transport 传输层接口
type Transport interface {
	// Connect 连接到服务器
	Connect(ctx context.Context) error

	// Send 发送消息
	Send(msg any) error

	// Receive 接收消息
	Receive() (any, error)

	// Close 关闭连接
	Close() error

	// IsConnected 是否已连接
	IsConnected() bool

	// RemoteAddr 远程地址
	RemoteAddr() string

	// SetTimeout 设置超时时间
	SetTimeout(timeout time.Duration)

	// SetReadDeadline 设置读取截止时间
	SetReadDeadline(deadline time.Duration)

	// SetWriteDeadline 设置写入截止时间
	SetWriteDeadline(deadline time.Duration)
}

// ConnTransport 基于 net.Conn 的传输层
type ConnTransport interface {
	Transport

	// GetConn 获取底层连接
	GetConn() net.Conn

	// SetConn 设置底层连接
	SetConn(conn net.Conn)
}

// Dialer 连接拨号器
type Dialer interface {
	// Dial 连接到目标地址
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// DialTimeout 带超时的连接
	DialTimeout(addr string, timeout time.Duration) (net.Conn, error)
}

// Listener 连接监听器
type Listener interface {
	// Listen 监听地址
	Listen(addr string) (net.Listener, error)

	// Accept 接受连接
	Accept() (net.Conn, error)

	// Close 关闭监听器
	Close() error
}

// Message 消息接口
type Message interface {
	// GetType 获取消息类型
	GetType() string

	// GetData 获取消息数据
	GetData() any

	// Marshal 序列化消息
	Marshal() ([]byte, error)

	// Unmarshal 反序列化消息
	Unmarshal(data []byte) error
}

// Handler 消息处理器
type Handler interface {
	// Handle 处理消息
	Handle(msg Message) error
}

// Middleware 传输中间件
type Middleware func(Transport) Transport
