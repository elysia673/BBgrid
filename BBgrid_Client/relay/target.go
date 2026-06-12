package relay

import (
	"BBgrid/common/mux"
	"context"
	"fmt"
	"net"
)

// targetRelay 目标端中继实现
type targetRelay struct {
	baseRelay
	mx *mux.Multiplexer
}

// newTargetRelay 创建目标端中继
func newTargetRelay(config Config, sender func(msg any) error) *targetRelay {
	return &targetRelay{
		baseRelay: baseRelay{
			config: config,
			state:  StateIdle,
			sender: sender,
		},
	}
}

// Start 启动目标端中继
func (r *targetRelay) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	r.setState(StateConnecting)

	go r.run(ctx)
	return nil
}

// Stop 停止目标端中继
func (r *targetRelay) Stop() error {
	r.setState(StateClosed)
	if r.cancel != nil {
		r.cancel()
	}
	if r.mx != nil {
		r.mx.Close()
	}
	r.sendClosed()
	return nil
}

// run 运行目标端中继
func (r *targetRelay) run(ctx context.Context) {
	defer func() {
		if rec := recover(); rec != nil {
			// log panic
		}
		r.Stop()
	}()

	// 连接到中继服务器
	wsConn, err := connectRelay(r.config)
	if err != nil {
		r.sendStatus("failed", err.Error())
		return
	}
	defer wsConn.Close()

	// 用 net.Pipe 隔离 mux 和 WebSocket
	muxSide, relaySide := net.Pipe()
	defer muxSide.Close()
	defer relaySide.Close()

	// relay 侧：WebSocket ↔ pipe
	wsAdapter := &wsConnAdapter{ws: wsConn}
	go func() {
		defer relaySide.Close()
		buf := make([]byte, 65536)
		for {
			n, err := wsAdapter.Read(buf)
			if err != nil {
				return
			}
			if _, err := relaySide.Write(buf[:n]); err != nil {
				return
			}
		}
	}()
	go func() {
		defer relaySide.Close()
		buf := make([]byte, 65536)
		for {
			n, err := relaySide.Read(buf)
			if err != nil {
				return
			}
			if _, err := wsAdapter.Write(buf[:n]); err != nil {
				return
			}
		}
	}()

	// mux 侧：建在 pipe 上
	localTarget := net.JoinHostPort(r.config.TargetLocalIP, fmt.Sprintf("%d", r.config.TargetPort))
	mx := mux.New(muxSide)
	mx.LocalTarget = localTarget
	mx.OnNewChannel = mx.HandleChannel
	r.mx = mx

	r.setState(StateConnected)
	r.sendStatus("connected", "")

	// context 取消时关闭
	go func() {
		<-ctx.Done()
		mx.Close()
	}()

	// 等待关闭
	<-mx.Done()
}
