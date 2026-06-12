package dataplane

import (
	alog "BBgrid/common/log"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

type Server struct {
	tunnelPort   int
	listeners    map[string]net.Listener
	lnMu         sync.Mutex
	stopCh       chan struct{}
	stopOnce     sync.Once
	onTunnelConn func(token string, conn net.Conn)
}

func NewServer(tunnelPort int) *Server {
	return &Server{
		tunnelPort: tunnelPort,
		listeners:  make(map[string]net.Listener),
		stopCh:     make(chan struct{}),
	}
}

func (s *Server) SetTunnelHandler(handler func(token string, conn net.Conn)) {
	s.onTunnelConn = handler
}

func (s *Server) Run() error {
	alog.Info(alog.CatSystem, "Data Plane 启动", "tunnel_port", s.tunnelPort)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.tunnelPort))
	if err != nil {
		return fmt.Errorf("listen tunnel port: %w", err)
	}
	s.lnMu.Lock()
	s.listeners["tunnel-tcp"] = ln
	s.lnMu.Unlock()

	alog.Info(alog.CatSystem, "TCP tunnel listener 启动", "port", s.tunnelPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return nil
			default:
				alog.Error(alog.CatSystem, "TCP Accept 失败", "error", err)
				continue
			}
		}
		go s.handleTunnelConn(conn)
	}
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.lnMu.Lock()
		for key, ln := range s.listeners {
			ln.Close()
			delete(s.listeners, key)
		}
		s.lnMu.Unlock()
	})
}

func (s *Server) handleTunnelConn(conn net.Conn) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		conn.Close()
		return
	}
	if string(header) != "TUNL" {
		conn.Close()
		return
	}

	var tokenLen uint16
	if err := binary.Read(conn, binary.BigEndian, &tokenLen); err != nil {
		conn.Close()
		return
	}

	token := make([]byte, tokenLen)
	if _, err := io.ReadFull(conn, token); err != nil {
		conn.Close()
		return
	}

	tokenStr := string(token)

	if s.onTunnelConn != nil {
		s.onTunnelConn(tokenStr, conn)
	} else {
		conn.Close()
	}
}
