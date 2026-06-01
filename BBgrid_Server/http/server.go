package http

import (
	"BBgrid/BBgrid_Server/auth"
	"BBgrid/BBgrid_Server/runtime"
	"BBgrid/BBgrid_Server/session"
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Server Control Plane HTTP 服务器
type Server struct {
	core      *runtime.Core
	session   *session.Server
	auth      *auth.Manager
	router    *gin.Engine
	version   string
	startTime time.Time
}

func NewServer(core *runtime.Core, sess *session.Server, authManager *auth.Manager, version string) *Server {
	return &Server{
		core:      core,
		session:   sess,
		auth:      authManager,
		router:    gin.Default(),
		version:   version,
		startTime: time.Now(),
	}
}

// SetupRoutes 设置路由
func (s *Server) SetupRoutes() {
	r := s.router

	// === 公开端点 ===
	r.GET("/PING", func(c *gin.Context) {
		c.String(200, "PANG")
	})

	r.GET("/status", s.handleStatus)
	r.GET("/health", s.handleHealth)

	// === Auth ===
	r.POST("/api/v1/auth/login", s.handleLogin)

	// === 注册 (公开) ===
	r.POST("/api/v1/register/apply", s.handleRegisterApply)
	r.POST("/api/v1/register/voucher", s.handleRegisterVoucher)
	r.GET("/api/v1/register/list", s.handleRegisterList)

	// === JWT 保护的端点 ===
	authGroup := r.Group("/api/v1", s.requireJWT())
	{
		// 节点管理
		authGroup.GET("/nodes", s.handleListNodes)
		authGroup.GET("/nodes/:id", s.handleGetNode)
		authGroup.POST("/register/approve", s.handleRegisterApprove)
		authGroup.POST("/register/revoke", s.handleRegisterRevoke)
		authGroup.GET("/register/pending", s.handleRegisterPending)

		// 凭证管理
		authGroup.POST("/vouchers", s.handleCreateVoucher)
		authGroup.GET("/vouchers", s.handleListVouchers)
		authGroup.DELETE("/vouchers/:code", s.handleDeleteVoucher)

		// 代理管理
		authGroup.GET("/proxies", s.handleListProxies)
		authGroup.POST("/proxies", s.handleCreateProxy)
		authGroup.DELETE("/proxies/:port", s.handleDeleteProxy)

		// 中继管理
		authGroup.GET("/relay", s.handleListRelays)
		authGroup.POST("/relay", s.handleCreateRelay)
		authGroup.DELETE("/relay/:id", s.handleDeleteRelay)

		// 命名空间
		authGroup.GET("/namespaces", s.handleListNamespaces)
		authGroup.GET("/namespaces/:name", s.handleGetNamespace)
		authGroup.GET("/namespaces/:name/clients", s.handleGetNamespaceClients)
		authGroup.POST("/namespaces/assign", s.handleAssignNamespace)

		// 任务管理
		authGroup.GET("/tasks/:id", s.handleGetTask)

		// Runtime 端点 (需要认证)
		authGroup.GET("/runtime/capabilities", s.handleCapabilities)
		authGroup.POST("/runtime/call", s.handleCall)
		authGroup.GET("/runtime/query", s.handleQuery)
	}

	// === WebSocket 端点 ===
	r.GET("/ws", gin.WrapF(s.session.Handle))
	r.GET("/ws/temp", gin.WrapF(s.session.HandleTemp))
	r.GET("/tunnel", gin.WrapF(s.session.HandleTunnel))
	r.GET("/relay", gin.WrapF(s.session.HandleRelay))
}

// Run 启动 HTTP 服务器
func (s *Server) Run(addr string) error {
	alog.Info(alog.CatSystem, "Control Plane 启动", "addr", addr)
	return s.router.Run(addr)
}

// RunTLS 启动 TLS HTTP 服务器
func (s *Server) RunTLS(addr, certFile, keyFile string, tlsConfig *tls.Config) error {
	alog.Info(alog.CatSystem, "Control Plane (TLS) 启动", "addr", addr)

	// 创建自定义 HTTP server 以支持 TLS 配置
	srv := &http.Server{
		Addr:      addr,
		Handler:   s.router,
		TLSConfig: tlsConfig,
	}

	return srv.ListenAndServeTLS(certFile, keyFile)
}

// ==================== Middleware ====================

// requireJWT JWT 认证中间件
func (s *Server) requireJWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		apiKey := c.GetHeader("X-API-KEY")
		clientToken := c.GetHeader("X-CLIENT-TOKEN")

		if authHeader != "" {
			if !strings.HasPrefix(authHeader, "Bearer ") {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid authorization format"})
				c.Abort()
				return
			}

			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			claims, err := s.auth.ValidateToken(tokenStr)
			if err != nil {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid token"})
				c.Abort()
				return
			}

			c.Set("api_key", claims.APIKey)
			c.Next()
			return
		}

		if apiKey != "" {
			if !s.auth.ValidateAPIKey(apiKey) {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid api key"})
				c.Abort()
				return
			}

			c.Set("api_key", apiKey)
			c.Next()
			return
		}

		if clientToken != "" {
			if !s.auth.ValidateClientToken(clientToken) {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid client token"})
				c.Abort()
				return
			}
			c.Next()
			return
		}

		c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "missing authorization"})
		c.Abort()
	}
}

// ==================== Runtime Handlers ====================

// handleCall 执行 Action
func (s *Server) handleCall(c *gin.Context) {
	var req struct {
		Action string         `json:"action"`
		Params map[string]any `json:"params"`
	}

	// 检查 Content-Type
	contentType := c.GetHeader("Content-Type")
	alog.Info(alog.CatSystem, "handleCall 收到请求",
		"content_type", contentType,
		"method", c.Request.Method,
		"path", c.Request.URL.Path,
		"query", c.Request.URL.RawQuery,
	)

	if strings.HasPrefix(contentType, "multipart/form-data") {
		// 处理 multipart/form-data 请求
		if err := c.Request.ParseMultipartForm(32 << 20); err != nil { // 32MB
			alog.Error(alog.CatSystem, "解析 multipart form 失败", "error", err)
			c.JSON(400, gin.H{"code": 400, "msg": "failed to parse multipart form: " + err.Error()})
			return
		}

		// 从 query 参数获取 action
		req.Action = c.Query("action")
		if req.Action == "" {
			c.JSON(400, gin.H{"code": 400, "msg": "missing action parameter"})
			return
		}

		// 构建 params
		req.Params = make(map[string]any)

		// 添加 query 参数
		if clientID := c.Query("client_id"); clientID != "" {
			req.Params["client_id"] = clientID
		}
		if fileType := c.Query("type"); fileType != "" {
			req.Params["type"] = fileType
		}

		// 添加 form 参数
		for key, values := range c.Request.MultipartForm.Value {
			if len(values) > 0 {
				req.Params[key] = values[0]
			}
		}

		// 处理上传的文件
		if file, header, err := c.Request.FormFile("file"); err == nil {
			defer file.Close()
			req.Params["file"] = file
			req.Params["filename"] = header.Filename
			req.Params["filesize"] = header.Size
			alog.Info(alog.CatSystem, "收到文件上传",
				"filename", header.Filename,
				"size", header.Size,
				"action", req.Action,
			)
		} else {
			alog.Warn(alog.CatSystem, "获取上传文件失败", "error", err)
		}
	} else {
		// 处理 JSON 请求
		if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
			return
		}
	}

	if req.Action == "" {
		c.JSON(400, gin.H{"code": 400, "msg": "missing action"})
		return
	}

	alog.Info(alog.CatSystem, "执行 action", "action", req.Action, "params_keys", getParamKeys(req.Params))

	ctx := &runtime.ActionContext{
		Action:  req.Action,
		Params:  req.Params,
		Headers: extractHeaders(c.Request),
	}

	result, err := s.core.Capability().Execute(ctx)
	if err != nil {
		c.JSON(500, gin.H{"code": 500, "msg": err.Error()})
		return
	}

	if result.Body != nil {
		defer result.Body.Close()
		c.Writer.Header().Set("Content-Type", "application/octet-stream")
		if result.BodyName != "" {
			c.Writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, result.BodyName))
		}
		if result.BodySize > 0 {
			c.Writer.Header().Set("Content-Length", fmt.Sprintf("%d", result.BodySize))
		}
		io.Copy(c.Writer, result.Body)
		return
	}

	ok(c, result)
}

// handleQuery 查询状态
func (s *Server) handleQuery(c *gin.Context) {
	queryType := c.Query("type")
	id := c.Query("id")

	state := s.core.StateStore()

	switch queryType {
	case "client":
		if id != "" {
			client, found := state.GetClientInfo(id)
			if !found {
				c.JSON(404, gin.H{"code": 404, "msg": "client not found"})
				return
			}
			ok(c, client)
		} else {
			ok(c, state.ListClients())
		}
	case "proxy":
		ok(c, state.ListProxies())
	case "relay":
		ok(c, state.ListRelaySessions())
	default:
		c.JSON(400, gin.H{"code": 400, "msg": "unknown query type"})
	}
}

// handleCapabilities 获取能力列表
func (s *Server) handleCapabilities(c *gin.Context) {
	caps := s.core.Capability().List()
	ok(c, caps)
}

// handleStatus 获取服务器状态
func (s *Server) handleStatus(c *gin.Context) {
	state := s.core.StateStore()
	uptime := time.Since(s.startTime)
	ok(c, gin.H{
		"status":     "ok",
		"version":    s.version,
		"public_ip":  s.core.StateStore().GetPublicIP(),
		"start_time": s.startTime.Format(time.RFC3339),
		"uptime":     formatDuration(uptime),
		"stats": gin.H{
			"clients": len(state.ListClients()),
			"proxies": len(state.ListProxies()),
			"relays":  len(state.ListRelaySessions()),
		},
	})
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

// ==================== Auth Handlers ====================

// handleLogin 登录
func (s *Server) handleLogin(c *gin.Context) {
	var req struct {
		APIKey string `json:"api_key"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if !s.auth.ValidateAPIKey(req.APIKey) {
		c.JSON(401, gin.H{"code": 401, "msg": "invalid api key"})
		return
	}
	token, err := s.auth.GenerateToken(req.APIKey)
	if err != nil {
		c.JSON(500, gin.H{"code": 500, "msg": "failed to generate token"})
		return
	}
	c.JSON(200, gin.H{
		"code": 0,
		"msg":  "success",
		"data": gin.H{
			"token":      token,
			"expires_in": 86400, // 24 hours in seconds
		},
	})
}

// handleRegisterApply 注册申请
func (s *Server) handleRegisterApply(c *gin.Context) {
	var req struct {
		ClientID  string `json:"client_id"`
		PublicKey string `json:"public_key"`
		Token     string `json:"token"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if !s.auth.ValidateClientToken(req.Token) {
		c.JSON(401, gin.H{"code": 401, "msg": "invalid token"})
		return
	}
	if err := s.auth.AddApplication(req.ClientID, req.PublicKey); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	c.JSON(200, gin.H{"code": 0, "msg": "applied"})
}

// handleRegisterVoucher 凭证注册：校验凭证 → 自动 approve → 签发证书
func (s *Server) handleRegisterVoucher(c *gin.Context) {
	var req struct {
		ClientID  string `json:"client_id"`
		PublicKey string `json:"public_key"`
		Voucher   string `json:"voucher"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if req.ClientID == "" || req.PublicKey == "" || req.Voucher == "" {
		c.JSON(400, gin.H{"code": 400, "msg": "client_id, public_key, voucher 都是必填"})
		return
	}

	// 校验凭证
	voucher, err := s.auth.UseVoucher(req.Voucher)
	if err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}

	// 添加客户端（已存在则更新公钥）
	if err := s.auth.AddApplication(req.ClientID, req.PublicKey); err != nil {
		if err.Error() != "client already exists" {
			c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
			return
		}
		// 已存在，更新公钥（用新密钥重新签发）
		if err := s.auth.UpdatePublicKey(req.ClientID, req.PublicKey); err != nil {
			c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
			return
		}
	}

	// 自动 approve（已 approved 也重新签发证书）
	namespace := voucher.Namespace
	if namespace == "" {
		namespace = "permanent"
	}
	role := voucher.Role
	if role == "" {
		role = "node"
	}

	cert, err := s.auth.Approve(req.ClientID, namespace, role)
	if err != nil {
		// 已 approved 的，先重置状态再重新签发
		if strings.Contains(err.Error(), "already approved") || strings.Contains(err.Error(), "already") {
			s.auth.ResetClientStatus(req.ClientID)
			cert, err = s.auth.Approve(req.ClientID, namespace, role)
			if err != nil {
				c.JSON(500, gin.H{"code": 500, "msg": err.Error()})
				return
			}
		} else {
			c.JSON(500, gin.H{"code": 500, "msg": err.Error()})
			return
		}
	}

	ok(c, gin.H{
		"certificate": string(cert),
		"namespace":   namespace,
		"role":        role,
	})
}

// handleRegisterList 注册列表 (已批准)
func (s *Server) handleRegisterList(c *gin.Context) {
	clients := s.auth.GetApproved()
	c.JSON(200, gin.H{
		"code": 0,
		"msg":  "success",
		"data": gin.H{
			"clients": clients,
		},
	})
}

// ==================== Node Handlers ====================

func (s *Server) handleListNodes(c *gin.Context) {
	state := s.core.StateStore()
	onlineClients := state.ListClients()

	onlineMap := make(map[string]bool, len(onlineClients))
	for _, oc := range onlineClients {
		onlineMap[oc.ID] = true
	}

	approved := s.auth.GetApproved()
	clients := make([]gin.H, 0, len(approved))
	for _, a := range approved {
		if onlineMap[a.ClientID] {
			for _, oc := range onlineClients {
				if oc.ID == a.ClientID {
					clients = append(clients, gin.H{
						"id":          oc.ID,
						"remote_addr": oc.RemoteAddr,
						"online":      true,
						"host":        oc.Host,
						"proxy_count": oc.ProxyCount,
					})
					break
				}
			}
		} else {
			clients = append(clients, gin.H{
				"id":          a.ClientID,
				"remote_addr": "",
				"online":      false,
				"host":        "",
				"proxy_count": 0,
			})
		}
	}

	ok(c, gin.H{"clients": clients})
}

func (s *Server) handleGetNode(c *gin.Context) {
	id := c.Param("id")
	state := s.core.StateStore()
	client, ok2 := state.GetClientInfo(id)
	if !ok2 {
		approved := s.auth.GetApproved()
		for _, a := range approved {
			if a.ClientID == id {
				ok(c, gin.H{
					"id":          a.ClientID,
					"remote_addr": "",
					"online":      false,
					"host":        "",
					"proxy_count": 0,
				})
				return
			}
		}
		c.JSON(404, gin.H{"code": 404, "msg": "client not found"})
		return
	}
	ok(c, client)
}

func (s *Server) handleRegisterApprove(c *gin.Context) {
	var req struct {
		ClientID  string `json:"client_id"`
		Namespace string `json:"namespace"`
		Role      string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	cert, err := s.auth.Approve(req.ClientID, req.Namespace, req.Role)
	if err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	ok(c, gin.H{"certificate": string(cert)})
}

func (s *Server) handleRegisterRevoke(c *gin.Context) {
	var req struct {
		ClientID string `json:"client_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if !s.auth.Delete(req.ClientID) {
		c.JSON(404, gin.H{"code": 404, "msg": "client not found"})
		return
	}
	ok(c, gin.H{"status": "revoked"})
}

func (s *Server) handleRegisterPending(c *gin.Context) {
	ok(c, gin.H{"applications": s.auth.GetPending()})
}

// ==================== Proxy Handlers ====================

func (s *Server) handleListProxies(c *gin.Context) {
	state := s.core.StateStore()
	ok(c, gin.H{"proxies": state.ListProxies()})
}

func (s *Server) handleCreateProxy(c *gin.Context) {
	var req struct {
		ClientID   string `json:"client_id"`
		RemotePort int    `json:"remote_port"`
		LocalPort  int    `json:"local_port"`
		LocalIP    string `json:"local_ip"`
		Protocol   string `json:"protocol"`
		BindAddr   string `json:"bind_addr"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}

	// 验证必填字段
	if req.ClientID == "" {
		c.JSON(400, gin.H{"code": 400, "msg": "client_id is required"})
		return
	}

	// 验证端口范围
	if req.RemotePort < 1 || req.RemotePort > 65535 {
		c.JSON(400, gin.H{"code": 400, "msg": "remote_port must be between 1 and 65535"})
		return
	}
	if req.LocalPort < 1 || req.LocalPort > 65535 {
		c.JSON(400, gin.H{"code": 400, "msg": "local_port must be between 1 and 65535"})
		return
	}

	// 验证协议
	if req.Protocol == "" {
		req.Protocol = "tcp"
	}
	if req.Protocol != "tcp" && req.Protocol != "udp" {
		c.JSON(400, gin.H{"code": 400, "msg": "protocol must be tcp or udp"})
		return
	}

	// 设置默认值
	if req.LocalIP == "" {
		req.LocalIP = "127.0.0.1"
	}
	if req.BindAddr == "" {
		req.BindAddr = "0.0.0.0"
	}

	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      fmt.Sprintf("%s:%d", req.ClientID, req.RemotePort),
		},
		proto.EventAdded,
		runtime.ProxyState{
			ClientID:   req.ClientID,
			RemotePort: req.RemotePort,
			LocalPort:  req.LocalPort,
			LocalIP:    req.LocalIP,
			Protocol:   req.Protocol,
			BindAddr:   req.BindAddr,
		},
	)
	s.core.Publish(event)
	alog.Info(alog.CatSystem, "proxy 创建事件已发布", "client_id", req.ClientID, "port", req.RemotePort)

	ok(c, gin.H{"status": "created"})
}

func (s *Server) handleDeleteProxy(c *gin.Context) {
	portStr := c.Param("port")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": "invalid port"})
		return
	}

	state := s.core.StateStore()

	// 直接遍历 proxies map 查找，不依赖 portIndex（portIndex 可能因事件丢失未填充）
	var targetKey string
	for _, p := range state.ListProxies() {
		if p.RemotePort == port {
			targetKey = fmt.Sprintf("%s:%d", p.ClientID, p.RemotePort)
			break
		}
	}
	if targetKey == "" {
		c.JSON(404, gin.H{"code": 404, "msg": "proxy not found"})
		return
	}

	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      targetKey,
		},
		proto.EventDeleted,
		nil,
	)
	s.core.Publish(event)

	ok(c, gin.H{"status": "deleted"})
}

// ==================== Relay Handlers ====================

func (s *Server) handleListRelays(c *gin.Context) {
	state := s.core.StateStore()
	ok(c, gin.H{"sessions": state.ListRelaySessions()})
}

func (s *Server) handleCreateRelay(c *gin.Context) {
	var req struct {
		SourceClientID string `json:"source_client_id"`
		TargetClientID string `json:"target_client_id"`
		Protocol       string `json:"protocol"`
		SourcePort     int    `json:"source_port"`
		TargetPort     int    `json:"target_port"`
		TargetLocalIP  string `json:"target_local_ip"`
		SourceLocalIP  string `json:"source_local_ip"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}

	if req.SourceClientID == "" || req.TargetClientID == "" {
		c.JSON(400, gin.H{"code": 400, "msg": "source_client_id and target_client_id are required"})
		return
	}
	if req.SourcePort < 1 || req.SourcePort > 65535 || req.TargetPort < 1 || req.TargetPort > 65535 {
		c.JSON(400, gin.H{"code": 400, "msg": "source_port and target_port must be between 1 and 65535"})
		return
	}
	if req.Protocol == "" {
		req.Protocol = "tcp"
	}
	if req.Protocol != "tcp" && req.Protocol != "udp" {
		c.JSON(400, gin.H{"code": 400, "msg": "protocol must be tcp or udp"})
		return
	}
	if req.TargetLocalIP == "" {
		req.TargetLocalIP = "127.0.0.1"
	}
	if req.SourceLocalIP == "" {
		req.SourceLocalIP = "0.0.0.0"
	}

	sessionID := proto.GenerateID()
	token := proto.GenerateID()
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      sessionID,
		},
		proto.EventAdded,
		runtime.RelaySession{
			ID:            sessionID,
			SourceClient:  req.SourceClientID,
			TargetClient:  req.TargetClientID,
			Protocol:      req.Protocol,
			SourcePort:    req.SourcePort,
			TargetPort:    req.TargetPort,
			TargetLocalIP: req.TargetLocalIP,
			SourceLocalIP: req.SourceLocalIP,
			Token:         token,
		},
	)
	s.core.Publish(event)

	ok(c, gin.H{"session_id": sessionID})
}

func (s *Server) handleDeleteRelay(c *gin.Context) {
	sessionID := c.Param("id")
	fullID := sessionID
	if _, exists := s.core.StateStore().GetRelaySession(sessionID); !exists {
		found := false
		for _, r := range s.core.StateStore().ListRelaySessions() {
			if strings.HasPrefix(r.ID, sessionID) {
				fullID = r.ID
				found = true
				break
			}
		}
		if !found {
			c.JSON(404, gin.H{"code": 404, "msg": "relay session not found"})
			return
		}
	}
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      fullID,
		},
		proto.EventDeleted,
		nil,
	)
	s.core.Publish(event)
	ok(c, gin.H{"status": "deleted"})
}

// ==================== Namespace Handlers ====================

func (s *Server) handleListNamespaces(c *gin.Context) {
	ok(c, gin.H{"namespaces": s.auth.ListNamespaces()})
}

func (s *Server) handleGetNamespace(c *gin.Context) {
	name := c.Param("name")
	ns := s.auth.GetNamespace(name)
	if ns == nil {
		c.JSON(404, gin.H{"code": 404, "msg": "namespace not found"})
		return
	}
	ok(c, ns)
}

func (s *Server) handleGetNamespaceClients(c *gin.Context) {
	name := c.Param("name")
	clients := s.auth.GetClientsByNamespace(name)
	ok(c, gin.H{"clients": clients})
}

func (s *Server) handleAssignNamespace(c *gin.Context) {
	var req struct {
		ClientID  string `json:"client_id"`
		Namespace string `json:"namespace"`
		Role      string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if err := s.auth.SetClientNamespace(req.ClientID, req.Namespace, req.Role); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	ok(c, gin.H{"status": "assigned"})
}

// ==================== Task Handlers ====================

func (s *Server) handleGetTask(c *gin.Context) {
	taskID := c.Param("id")
	// 当前没有任务系统，返回 404
	// 未来可以实现异步任务跟踪
	c.JSON(404, gin.H{
		"code": 404,
		"msg":  "task not found",
		"data": gin.H{
			"task_id": taskID,
			"status":  "not_found",
		},
	})
}

// ==================== Helpers ====================

func extractHeaders(r *http.Request) map[string]string {
	headers := make(map[string]string)
	for k := range r.Header {
		headers[k] = r.Header.Get(k)
	}
	return headers
}

func getParamKeys(params map[string]any) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	return keys
}

// ==================== Voucher Handlers ====================

func (s *Server) handleCreateVoucher(c *gin.Context) {
	var req struct {
		MaxUses   int    `json:"max_uses"`
		ExpiresIn int    `json:"expires_in"` // 秒，0=不过期
		Namespace string `json:"namespace"`
		Role      string `json:"role"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"code": 400, "msg": err.Error()})
		return
	}
	if req.Namespace == "" {
		req.Namespace = "permanent"
	}
	if req.Role == "" {
		req.Role = "node"
	}
	var expiresAt int64
	if req.ExpiresIn > 0 {
		expiresAt = time.Now().Add(time.Duration(req.ExpiresIn) * time.Second).Unix()
	}
	v, err := s.auth.CreateVoucher(req.MaxUses, expiresAt, req.Namespace, req.Role)
	if err != nil {
		c.JSON(500, gin.H{"code": 500, "msg": err.Error()})
		return
	}
	ok(c, v)
}

func (s *Server) handleListVouchers(c *gin.Context) {
	ok(c, gin.H{"vouchers": s.auth.ListVouchers()})
}

func (s *Server) handleDeleteVoucher(c *gin.Context) {
	code := c.Param("code")
	if !s.auth.DeleteVoucher(code) {
		c.JSON(404, gin.H{"code": 404, "msg": "voucher not found"})
		return
	}
	ok(c, gin.H{"status": "deleted"})
}

// ok 返回标准成功响应 {"code":0,"msg":"success","data":...}
func ok(c *gin.Context, data any) {
	c.JSON(200, gin.H{"code": 0, "msg": "success", "data": data})
}
