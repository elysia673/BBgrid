package relay

import (
	"BBgrid/common/mux"
	"context"
	"fmt"
	"net"
)

// sourceRelay 源端中继实现
type sourceRelay struct {
	baseRelay
	ln net.Listener
	mx *mux.Multiplexer
}

// newSourceRelay 创建源端中继
func newSourceRelay(config Config, sender func(msg any) error) *sourceRelay {
	return &sourceRelay{
		baseRelay: baseRelay{
			config: config,
			state:  StateIdle,
			sender: sender,
		},
	}
}

// Start 启动源端中继
func (r *sourceRelay) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)
	r.setState(StateConnecting)

	go r.run(ctx)
	return nil
}

// Stop 停止源端中继
func (r *sourceRelay) Stop() error {
	r.setState(StateClosed)
	if r.cancel != nil {
		r.cancel()
	}
	if r.ln != nil {
		r.ln.Close()
	}
	if r.mx != nil {
		r.mx.Close()
	}
	r.sendClosed()
	return nil
}

// run 运行源端中继
func (r *sourceRelay) run(ctx context.Context) {
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
	mx := mux.New(muxSide)
	r.mx = mx

	// 监听本地端口
	bindAddr := net.JoinHostPort(r.config.SourceLocalIP, fmt.Sprintf("%d", r.config.SourcePort))
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		r.sendStatus("failed", err.Error())
		return
	}
	r.ln = ln
	defer ln.Close()

	r.setState(StateConnected)
	r.sendStatus("connected", "")

	// context 取消时关闭
	go func() {
		<-ctx.Done()
		ln.Close()
		mx.Close()
	}()

	// 接受本地连接并转发
	for {
		localConn, err := ln.Accept()
		if err != nil {
			return
		}

		channel, err := mx.OpenChannel(uint16(r.config.SourcePort))
		if err != nil {
			localConn.Close()
			return
		}

		go bridgeChannel(localConn, channel, r.config.SessionID)
	}
}
