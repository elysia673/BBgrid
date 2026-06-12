package tunnel

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// udpTunnel UDP 隧道实现
type udpTunnel struct {
	baseTunnel
}

// newUDPTunnel 创建 UDP 隧道
func newUDPTunnel(config Config) *udpTunnel {
	return &udpTunnel{
		baseTunnel: baseTunnel{
			config:    config,
			state:     StateIdle,
			localIP:   config.LocalIP,
			localPort: config.LocalPort,
		},
	}
}

// Start 启动 UDP 隧道
func (t *udpTunnel) Start(ctx context.Context) error {
	ctx, t.cancel = context.WithCancel(ctx)
	t.setState(StateConnecting)

	go t.run(ctx)
	return nil
}

// Stop 停止 UDP 隧道
func (t *udpTunnel) Stop() error {
	t.setState(StateClosed)
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}

// run 运行 UDP 隧道
func (t *udpTunnel) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.connectAndForward(ctx); err != nil {
			t.setState(StateConnecting)
			time.Sleep(1 * time.Second)
			continue
		}

		t.setState(StateClosed)
		return
	}
}

// connectAndForward 连接并转发数据
func (t *udpTunnel) connectAndForward(ctx context.Context) error {
	tunnelAddr := net.JoinHostPort(t.config.ServerHost, fmt.Sprintf("%d", t.config.TunnelPort))

	// 连接到服务器隧道端口
	conn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
	if err != nil {
		return fmt.Errorf("tunnel dial: %w", err)
	}
	defer conn.Close()

	// 发送认证标记 "TUNL"
	if _, err := conn.Write([]byte("TUNL")); err != nil {
		return fmt.Errorf("tunnel auth marker: %w", err)
	}

	// 发送 token 长度和 token
	tokenBytes := []byte(t.config.Token)
	tokenLen := uint16(len(tokenBytes))
	if err := binary.Write(conn, binary.BigEndian, tokenLen); err != nil {
		return fmt.Errorf("tunnel token length: %w", err)
	}
	if _, err := conn.Write(tokenBytes); err != nil {
		return fmt.Errorf("tunnel token: %w", err)
	}

	// 等待确认
	ack := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, ack); err != nil {
		return fmt.Errorf("tunnel ack: %w", err)
	}
	conn.SetReadDeadline(time.Time{})

	if ack[0] != 0x01 {
		return fmt.Errorf("tunnel ack invalid: 0x%02x", ack[0])
	}

	t.setState(StateConnected)

	// 启动 UDP 数据转发
	t.handleUDPTunnel(ctx, conn)

	return nil
}

// handleUDPTunnel 处理 UDP 隧道数据转发
func (t *udpTunnel) handleUDPTunnel(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	// 连接到本地 UDP 服务
	localAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(t.localIP, fmt.Sprintf("%d", t.localPort)))
	if err != nil {
		return
	}

	localConn, err := net.DialUDP("udp", nil, localAddr)
	if err != nil {
		return
	}
	defer localConn.Close()

	// 从服务器读取 UDP 数据并转发到本地
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer localConn.Close()
		buf := make([]byte, 65535)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// 读取 UDP 数据头：[2字节目标端口][2字节数据长度]
			var destPort uint16
			var dataLen uint16

			if err := binary.Read(conn, binary.BigEndian, &destPort); err != nil {
				return
			}

			if err := binary.Read(conn, binary.BigEndian, &dataLen); err != nil {
				return
			}

			if dataLen > 65535 {
				return
			}

			data := buf[:dataLen]
			if _, err := io.ReadFull(conn, data); err != nil {
				return
			}

			// 发送到本地 UDP 服务
			if _, err := localConn.Write(data); err != nil {
				continue
			}
		}
	}()

	// 从本地 UDP 服务读取响应并发送回服务器
	buf := make([]byte, 65535)
	for {
		select {
		case <-ctx.Done():
			<-done
			return
		case <-done:
			return
		default:
		}

		n, err := localConn.Read(buf)
		if err != nil {
			break
		}

		if n == 0 {
			continue
		}

		// 获取本地端口（作为源端口）
		localPort := localConn.LocalAddr().(*net.UDPAddr).Port

		// 发送响应到服务器
		// 格式：[2字节源端口][2字节数据长度][数据]
		packet := make([]byte, 4+n)
		binary.BigEndian.PutUint16(packet[0:2], uint16(localPort))
		binary.BigEndian.PutUint16(packet[2:4], uint16(n))
		copy(packet[4:], buf[:n])

		if _, err := conn.Write(packet); err != nil {
			break
		}
	}

	<-done
}
