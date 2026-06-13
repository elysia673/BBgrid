// Package transport 提供传输层抽象，支持多种传输协议。
package transport

import (
	"context"
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
