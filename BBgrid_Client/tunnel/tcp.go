package tunnel

import (
	"BBgrid/common/proto"
	"context"
	"fmt"
	"io"
	"net"
	"time"
)

// tcpTunnel TCP 隧道实现
type tcpTunnel struct {
	baseTunnel
}

// newTCPTunnel 创建 TCP 隧道
func newTCPTunnel(config Config) *tcpTunnel {
	return &tcpTunnel{
		baseTunnel: baseTunnel{
			config:    config,
			state:     StateIdle,
			localIP:   config.LocalIP,
			localPort: config.LocalPort,
		},
	}
}

// Start 启动 TCP 隧道
func (t *tcpTunnel) Start(ctx context.Context) error {
	ctx, t.cancel = context.WithCancel(ctx)
	t.setState(StateConnecting)

	go t.run(ctx)
	return nil
}

// Stop 停止 TCP 隧道
func (t *tcpTunnel) Stop() error {
	t.setState(StateClosed)
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

// run 运行 TCP 隧道
func (t *tcpTunnel) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.connectAndPipe(ctx); err != nil {
			t.setState(StateConnecting)
			time.Sleep(1 * time.Second)
			continue
		}

		t.setState(StateClosed)
		return
	}
}

// connectAndPipe 连接并转发数据
func (t *tcpTunnel) connectAndPipe(ctx context.Context) error {
	tunnelAddr := net.JoinHostPort(t.config.ServerHost, fmt.Sprintf("%d", t.config.TunnelPort))

	// 连接到服务器隧道端口
	tunnelConn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("tunnel dial: %w", err)
	}
	defer tunnelConn.Close()

	// 发送认证
	if err := proto.WriteTunnelAuth(tunnelConn, t.config.Token); err != nil {
		return fmt.Errorf("tunnel auth: %w", err)
	}

	// 等待确认
	ack := make([]byte, 1)
	tunnelConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(tunnelConn, ack); err != nil || ack[0] != 0x01 {
		return fmt.Errorf("tunnel ack: %w", err)
	}
	tunnelConn.SetReadDeadline(time.Time{})

	t.setState(StateConnected)

	// 连接本地服务
	localAddr := net.JoinHostPort(t.localIP, fmt.Sprintf("%d", t.localPort))
	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("local dial: %w", err)
	}
	defer localConn.Close()

	// context 取消时关闭连接
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tunnelConn.Close()
			localConn.Close()
		case <-done:
		}
	}()

	// 双向转发
	pipeTCP(tunnelConn, localConn)
	close(done)

	return nil
}
