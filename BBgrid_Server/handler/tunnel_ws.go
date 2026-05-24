package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/mux"
	"BBgrid/common/wsconn"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/gin-gonic/gin"
)

func (h *WSHandler) HandleTunnelWS(c *gin.Context) {
	conn, err := newUpgrader(h.domain).Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		alog.Error(alog.CatTunnel, "websocket upgrade error", "error", err)
		return
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		alog.Error(alog.CatAuth, "tunnel auth read error", "error", err)
		conn.Close()
		return
	}

	var auth model.TunnelAuthMsg
	if err := json.Unmarshal(msg, &auth); err != nil {
		alog.Error(alog.CatAuth, "tunnel auth json error", "error", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid auth"})
		conn.Close()
		return
	}

	if auth.Type != "tunnel_auth" {
		alog.Warn(alog.CatAuth, "tunnel auth unexpected type", "type", auth.Type)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "unexpected message type"})
		conn.Close()
		return
	}

	table, key, err := h.clientMgr.FindTableByWSToken(auth.Data.Token)
	if err != nil {
		alog.Error(alog.CatAuth, "tunnel token invalid", "error", err)
		conn.WriteJSON(map[string]string{"type": "tunnel_error", "data": "invalid token"})
		conn.Close()
		return
	}

	wsAdapter := wsconn.New(conn)

	mx := mux.New(wsAdapter)

	ready := model.TunnelReadyMsg{
		Type: "tunnel_ready",
		Data: model.TunnelReadyData{Status: "ok"},
	}
	if err := conn.WriteJSON(ready); err != nil {
		alog.Error(alog.CatTunnel, "tunnel ready write error", "error", err)
		mx.Close()
		return
	}

	table.PutMultiplexer(key, mx)
	alog.Info(alog.CatMux, "tunnel multiplexer created via websocket", "key", key)

	<-mx.Done()
	table.RemoveMux(key, mx)
	table.RemoveWSToken(auth.Data.Token)
	alog.Info(alog.CatMux, "tunnel multiplexer closed", "key", key)
}

// StartWSProxy 启动 WebSocket 代理监听
func (h *APIHandler) StartWSProxy(port int, bindAddr string, table *manager.ClientTable, token string) {
	key := table.TunnelKey(port)
	clientID := table.ClientID()

	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		alog.Error(alog.CatProxy, "websocket proxy listen error", "error", err, "port", port)
		return
	}
	defer ln.Close()

	proxy := table.GetProxy(port)
	if proxy != nil {
		table.AddProxy(&manager.ProxyInfo{
			RemotePort: proxy.RemotePort,
			LocalPort:  proxy.LocalPort,
			LocalIP:    proxy.LocalIP,
			Protocol:   proxy.Protocol,
			BindAddr:   proxy.BindAddr,
			Listener:   ln,
		})
	}

	defer func() {
		table.RemoveTunnel(key)
		table.RemoveWSToken(token)
	}()

	alog.Info(alog.CatProxy, "websocket proxy listening", "port", port, "client_id", clientID)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		go handleWSConnection(conn, key, table)
	}
}

func handleWSConnection(conn net.Conn, key string, table *manager.ClientTable) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatProxy, "handleWSConnection panic", "error", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatProxy, "websocket proxy public connection", "remote", remoteAddr, "key", key)

	mx, err := table.GetMultiplexer(key)
	if err != nil {
		alog.Error(alog.CatMux, "no multiplexer available", "key", key, "error", err)
		conn.Close()
		return
	}

	port := table.GetProxyPort(key)
	if port == 0 {
		alog.Error(alog.CatProxy, "cannot get port", "key", key)
		conn.Close()
		return
	}

	channel, err := mx.OpenChannel(uint16(port))
	if err != nil {
		alog.Error(alog.CatMux, "create channel failed", "key", key, "error", err)
		conn.Close()
		return
	}

	alog.Info(alog.CatMux, "channel opened", "port", channel.Port, "remote", remoteAddr)

	go handleChannel(conn, channel, key)
}
