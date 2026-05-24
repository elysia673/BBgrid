// Package handler 提供 HTTP/WebSocket 请求处理
//
// 包含 API 接口、WebSocket 连接管理和代理隧道处理。
package handler

import (
	"BBgrid/BBgrid_Server/manager"
	"BBgrid/BBgrid_Server/middleware"
	"BBgrid/BBgrid_Server/storage"
	"BBgrid/common/config"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// APIHandler 处理 REST API 请求
type APIHandler struct {
	clientMgr  *manager.ClientManager // 客户端管理器
	domain     string                 // 公网域名
	tunnelPort int                    // 隧道端口
	store      *storage.Storage       // 持久化存储
	cfg        *config.ServerConfig
}

type LoginRequest struct {
	APIKey string `json:"api_key" binding:"required"`
}

type LoginResponse struct {
	Token     string `json:"token"`
	ExpiresIn int64  `json:"expires_in"`
}

// NewAPIHandler 创建 API 处理器
func NewAPIHandler(clientMgr *manager.ClientManager, domain string, tunnelPort int, store *storage.Storage, cfg *config.ServerConfig) *APIHandler {
	return &APIHandler{
		clientMgr:  clientMgr,
		domain:     domain,
		tunnelPort: tunnelPort,
		store:      store,
		cfg:        cfg,
	}
}

// ListClients 返回所有已连接的客户端列表
func (h *APIHandler) ListClients(c *gin.Context) {
	clients := h.clientMgr.ListClients()
	c.JSON(http.StatusOK, model.Success(gin.H{
		"clients": clients,
	}))
}

// proxyRequest 创建代理的请求参数
type proxyRequest struct {
	RemotePort int    `json:"remote_port" binding:"required,min=1,max=65535"`                 // 服务端暴露端口
	LocalPort  int    `json:"local_port" binding:"required,min=1,max=65535"`                  // 客户端本地端口
	Protocol   string `json:"protocol" binding:"required,oneof=tcp udp http https websocket"` // 协议类型
	BindAddr   string `json:"bind_addr"`                                                      // 服务端绑定地址
	LocalIP    string `json:"local_ip"`                                                       // 客户端本地 IP
}

// CreateProxy 创建代理映射
//
// 流程：
// 1. 验证请求参数
// 2. 生成隧道认证 token
// 3. 向客户端发送代理命令
// 4. 启动服务端代理监听
// 5. 返回公网访问地址
func (h *APIHandler) CreateProxy(c *gin.Context) {
	clientID := c.Param("id")
	var req proxyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	tunnelHost := table.TunnelHost(h.clientMgr.GetPublicIP())

	token := generateToken()

	cmdData, err := json.Marshal(model.CommandData{
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
		ServerHost: tunnelHost,
		TunnelPort: h.tunnelPort,
		Token:      token,
		LocalIP:    req.LocalIP,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to marshal command"))
		return
	}
	publicAddr := h.domain
	if publicAddr == "" {
		publicAddr = h.clientMgr.GetPublicIP()
	}

	cmd := model.WSMessage{
		Type: "proxy",
		Data: string(cmdData),
	}

	result := make(chan error, 1)
	go func() {
		result <- h.clientMgr.SendCommand(clientID, cmd)
	}()

	select {
	case err := <-result:
		if err != nil {
			c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send command: "+err.Error()))
			return
		}
	case <-time.After(8 * time.Second):
		alog.Warn(alog.CatProxy, "send proxy command taking too long, continuing", "clientID", clientID)
	}

	table.AddProxy(&manager.ProxyInfo{
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		LocalIP:    req.LocalIP,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
	})
	h.clientMgr.RegisterPort(clientID, req.RemotePort)

	h.store.Add(storage.ProxyRecord{
		ClientID:   clientID,
		RemotePort: req.RemotePort,
		LocalPort:  req.LocalPort,
		LocalIP:    req.LocalIP,
		Protocol:   req.Protocol,
		BindAddr:   req.BindAddr,
	})

	if req.Protocol == "websocket" {
		table.StoreWSToken(token, fmt.Sprintf("%s-%d", clientID, req.RemotePort))
		go h.StartWSProxy(req.RemotePort, req.BindAddr, table, token)
	} else if req.Protocol == "udp" {
		go h.StartUDPProxy(req.RemotePort, req.BindAddr, table, token)
	} else {
		table.StoreTunnelToken(token, fmt.Sprintf("%s-%d", clientID, req.RemotePort))
		go h.StartTCPProxy(req.RemotePort, req.BindAddr, table, token)
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"public_addr": publicAddr + ":" + strconv.Itoa(req.RemotePort),
	}))
}

// generateToken 生成随机令牌。
func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}

func (h *APIHandler) GetClientInfo(c *gin.Context) {
	clientID := c.Param("id")

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	proxies := table.ListProxies()
	ports := make([]gin.H, 0, len(proxies))
	for _, p := range proxies {
		ports = append(ports, gin.H{
			"remote_port": p.RemotePort,
			"local_port":  p.LocalPort,
			"local_ip":    p.LocalIP,
			"protocol":    p.Protocol,
			"bind_addr":   p.BindAddr,
		})
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"client_id": clientID,
		"ports":     ports,
	}))
}

func (h *APIHandler) HandleLogin(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	if req.APIKey != h.cfg.Auth.APIKey {
		c.JSON(http.StatusUnauthorized, model.Error(401, "invalid API key"))
		return
	}

	token, err := middleware.GenerateToken(req.APIKey)
	if err != nil {
		c.JSON(http.StatusConflict, model.Error(409, err.Error()))
		return
	}

	c.JSON(http.StatusOK, model.Success(LoginResponse{
		Token:     token,
		ExpiresIn: 365 * 24 * 3600,
	}))
}

// HandleUpdateServer 更新服务端自身
func (h *APIHandler) HandleUpdateServer(c *gin.Context) {
	file, _, err := c.Request.FormFile("binary")
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "missing binary file"))
		return
	}
	defer file.Close()

	expectedMD5 := c.PostForm("md5")

	execPath, err := os.Executable()
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "get executable path failed"))
		return
	}

	tmpPath := execPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "create temp file failed"))
		return
	}

	hash := md5.New()
	writer := io.MultiWriter(tmpFile, hash)

	if _, err := io.Copy(writer, file); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		c.JSON(http.StatusInternalServerError, model.Error(500, "write binary failed"))
		return
	}
	tmpFile.Close()

	actualMD5 := hex.EncodeToString(hash.Sum(nil))

	if expectedMD5 != "" && actualMD5 != expectedMD5 {
		os.Remove(tmpPath)
		c.JSON(http.StatusBadRequest, model.Error(400, fmt.Sprintf("md5 mismatch: expected %s, got %s", expectedMD5, actualMD5)))
		return
	}

	if err := os.Rename(tmpPath, execPath); err != nil {
		os.Remove(tmpPath)
		c.JSON(http.StatusInternalServerError, model.Error(500, "replace binary failed"))
		return
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message": "binary updated, restarting...",
		"md5":     actualMD5,
	}))

	alog.Info(alog.CatUpdate, "received update, restarting", "md5", actualMD5)

	// 优雅重启：等待响应发完后退出，由 systemd 拉起新进程
	go func() {
		time.Sleep(1 * time.Second)
		alog.Info(alog.CatUpdate, "gracefully exiting, waiting for connections to drain")
		// 给现有连接 10 秒排空时间
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}()
}

// HandleClientUpdate 更新指定客户端
func (h *APIHandler) HandleClientUpdate(c *gin.Context) {
	clientID := c.Param("id")

	table, ok := h.clientMgr.Get(clientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "client not found"))
		return
	}

	conn := table.Conn()
	if conn == nil {
		c.JSON(http.StatusNotFound, model.Error(404, "client not connected"))
		return
	}

	fileHeader, err := c.FormFile("binary")
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "missing binary file"))
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "open file failed"))
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "read file failed"))
		return
	}

	hash := md5.Sum(data)
	md5sum := hex.EncodeToString(hash[:])

	totalChunks := (len(data) + 16*1024 - 1) / (16 * 1024)

	// 发送开始消息
	startMsg := model.WSMessage{
		Type: "update_start",
		Data: map[string]interface{}{
			"platform": "linux",
			"md5":      md5sum,
			"size":     len(data),
			"chunks":   totalChunks,
		},
	}
	if err := conn.WriteJSON(&startMsg); err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "send update_start failed"))
		return
	}

	// 发送数据块
	chunkSize := 16 * 1024
	for i := 0; i < totalChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[start:end]
		chunkMsg := model.WSMessage{
			Type: "update_chunk",
			Data: map[string]interface{}{
				"index": i,
				"data":  base64.StdEncoding.EncodeToString(chunk),
			},
		}
		if err := conn.WriteJSON(&chunkMsg); err != nil {
			c.JSON(http.StatusInternalServerError, model.Error(500, "send chunk failed"))
			return
		}
	}

	// 发送结束消息
	endMsg := model.WSMessage{
		Type: "update_end",
		Data: map[string]interface{}{},
	}
	if err := conn.WriteJSON(&endMsg); err != nil {
		c.JSON(http.StatusInternalServerError, model.Error(500, "send update_end failed"))
		return
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message":  "update sent to client",
		"client":   clientID,
		"md5":      md5sum,
		"size":     len(data),
		"chunks":   totalChunks,
		"platform": "linux",
	}))
}
