package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *APIHandler) CloseProxy(c *gin.Context) {
	portStr := c.Param("port")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid port"))
		return
	}

	clientID, ok := h.clientMgr.GetClientIDByPort(port)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "proxy not found"))
		return
	}

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	key := table.TunnelKey(port)

	conn := table.Conn()

	result := make(chan error, 1)
	go func() {
		result <- h.cleanupProxy(table, port, key, clientID)
	}()

	select {
	case err := <-result:
		if err != nil {
			c.JSON(http.StatusInternalServerError, model.Error(500, "cleanup failed: "+err.Error()))
			return
		}
	case <-time.After(8 * time.Second):
		alog.Warn(alog.CatProxy, "cleanup taking too long, continuing", "port", port)
	}

	go func() {
		notifyMsg := model.WSMessage{
			Type: "proxy_closed",
			Data: map[string]string{"key": key},
		}
		conn.WriteJSON(&notifyMsg)
	}()

	c.JSON(http.StatusOK, model.Success(gin.H{"message": "proxy closed"}))
}

func (h *APIHandler) cleanupProxy(table *manager.ClientTable, port int, key string, clientID string) error {
	alog.Info(alog.CatProxy, "cleanup start", "port", port, "key", key)

	t0 := time.Now()
	ln := table.GetProxyListener(port)
	alog.Info(alog.CatProxy, "cleanup step1 getListener", "port", port, "elapsed", time.Since(t0))

	if ln != nil {
		t1 := time.Now()
		if err := ln.Close(); err != nil {
			alog.Error(alog.CatProxy, "close listener failed", "port", port, "error", err, "elapsed", time.Since(t1))
		} else {
			alog.Info(alog.CatProxy, "close listener ok", "port", port, "elapsed", time.Since(t1))
		}
	}

	t2 := time.Now()
	table.RemoveTunnel(key)
	alog.Info(alog.CatProxy, "cleanup step2 removeTunnel", "port", port, "elapsed", time.Since(t2))

	t3 := time.Now()
	table.RemoveProxy(port)
	table.RemoveTunnelTokenByKey(key)
	h.clientMgr.UnregisterPort(port)
	alog.Info(alog.CatProxy, "cleanup step3 removeState", "port", port, "elapsed", time.Since(t3))

	t4 := time.Now()
	if err := h.store.Remove(clientID, port); err != nil {
		alog.Error(alog.CatProxy, "remove from storage failed", "port", port, "error", err, "elapsed", time.Since(t4))
	} else {
		alog.Info(alog.CatProxy, "cleanup step4 removeStorage", "port", port, "elapsed", time.Since(t4))
	}

	alog.Info(alog.CatProxy, "cleanup done", "port", port)
	return nil
}

func (h *APIHandler) ListProxies(c *gin.Context) {
	proxies := h.clientMgr.ListAllProxies()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"proxies": proxies,
	}))
}
