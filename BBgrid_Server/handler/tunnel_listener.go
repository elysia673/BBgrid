package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"errors"
	"fmt"
	"net"
	"time"
)

// TunnelListener 隧道监听器
type TunnelListener struct {
	listener  net.Listener
	clientMgr *manager.ClientManager
	api       *APIHandler
}

// NewTunnelListener 创建隧道监听器
func NewTunnelListener(port int, clientMgr *manager.ClientManager, api *APIHandler) (*TunnelListener, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen tunnel port: %w", err)
	}

	return &TunnelListener{
		listener:  ln,
		clientMgr: clientMgr,
		api:       api,
	}, nil
}

// Start 启动隧道监听器
func (tl *TunnelListener) Start() {
	alog.Info(alog.CatTunnel, "tunnel listener started", "addr", tl.listener.Addr())

	for {
		conn, err := tl.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			alog.Error(alog.CatTunnel, "tunnel accept error", "error", err)
			continue
		}

		go tl.handleTunnelConn(conn)
	}
}

// Close 关闭隧道监听器
func (tl *TunnelListener) Close() {
	tl.listener.Close()
}

func (tl *TunnelListener) handleTunnelConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "handleTunnelConn panic", "error", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatTunnel, "new tunnel connection", "remote", remoteAddr)

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	token, err := proto.ReadTunnelAuth(conn)
	conn.SetReadDeadline(time.Time{})

	if err != nil {
		alog.Error(alog.CatAuth, "tunnel auth failed", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}

	tl.api.AcceptTunnel(conn, token)
}
