// Package main 是 BBgrid Server 入口。
// 负责加载配置、初始化 Worker、启动 HTTP/gRPC/Tunnel 服务。
package main

import (
	"BBgrid/BBgrid_Server/workers"
	"BBgrid/common/config"
	alog "BBgrid/common/log"
	"BBgrid/common/middleware"
	"BBgrid/common/plugin"
	pluginpb "BBgrid/common/plugin/proto/pb"
	"BBgrid/common/proto"
	"BBgrid/common/store"
	_ "BBgrid/plugins/file"
	_ "BBgrid/plugins/latency"
	_ "BBgrid/plugins/persist"
	_ "BBgrid/plugins/tag"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

// 版本信息变量，编译时通过 -ldflags 注入
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// printVersion 打印版本信息并退出
func printVersion() {
	fmt.Printf("BBgrid_Server %s (%s) %s\n", Version, GitCommit, BuildTime)
}

// getPublicIP 获取当前服务器的公网 IP 地址
// 依次尝试多个外部 API，失败则回退到本机网络接口
func getPublicIP() string {
	addrs := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
	}
	httpClient := &http.Client{Timeout: 5 * time.Second}
	for _, url := range addrs {
		resp, err := httpClient.Get(url)
		if err != nil {
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}
		ip := strings.TrimSpace(string(body))
		if net.ParseIP(ip) != nil {
			return ip
		}
	}
	// 外部 API 均失败，回退到本机网络接口
	ifaces, err := net.Interfaces()
	if err == nil {
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok {
					ip := ipnet.IP
					if ip != nil && ip.To4() != nil && !ip.IsLoopback() {
						return ip.String()
					}
				}
			}
		}
	}
	return ""
}

func main() {
	// ===== 配置加载 =====
	configPath := flag.String("config", "config.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		printVersion()
		return
	}

	cfg, err := config.LoadServer(*configPath)
	if err != nil {
		alog.Fatal(alog.CatConfig, "加载配置失败", "error", err)
	}

	// ===== 日志初始化 =====
	if cfg.LogPath != "" {
		if err := alog.SetFile(cfg.LogPath); err != nil {
			alog.Fatal(alog.CatConfig, "初始化日志文件失败", "error", err, "path", cfg.LogPath)
		}
		alog.Info(alog.CatConfig, "日志文件已启用", "path", cfg.LogPath)
	}

	// ===== 公网 IP 检测 =====
	publicIP := cfg.PublicIP
	if publicIP == "" {
		alog.Info(alog.CatSystem, "正在获取公网IP")
		publicIP = getPublicIP()
	}
	alog.Info(alog.CatSystem, "公网IP已确定", "publicIP", publicIP)

	// ===== 状态收集器创建 =====
	statusCollector := NewStatusCollector(Version, publicIP)

	// ===== 事件分发器创建 =====
	dispatcher := NewDispatcher()
	alog.Info(alog.CatSystem, "事件分发器已创建")

	// ===== 存储管理器创建 =====
	storageManager, err := store.NewStorageManager(store.StorageConfig{
		DataDir:          cfg.DataDir,
		SnapshotInterval: 1000,
	})
	if err != nil {
		alog.Fatal(alog.CatSystem, "创建存储管理器失败", "error", err)
	}
	defer storageManager.Close()
	alog.Info(alog.CatSystem, "存储管理器已创建")

	// ===== Worker 创建：认证、状态、数据、控制、WebSocket =====
	authWorker := workers.NewAuthWorker(workers.AuthConfig{
		DataDir:     cfg.DataDir,
		APIKey:      cfg.Auth.APIKey,
		ClientToken: cfg.Auth.ClientToken,
	})

	stateWorker := workers.NewStateWorker(workers.StateConfig{
		PublicIP:     publicIP,
		PingInterval: 30 * time.Second,
	}, dispatcher, storageManager)

	dataWorker := workers.NewDataWorker(workers.DataConfig{
		TunnelPort: cfg.Server.TunnelPort,
	}, stateWorker)

	controlWorker := workers.NewControlWorker(workers.ControlConfig{
		Domain:     cfg.Server.Domain,
		TunnelPort: cfg.Server.TunnelPort,
		APIKey:     cfg.Auth.APIKey,
	}, stateWorker, authWorker, authWorker, dataWorker)

	wsWorker := workers.NewWSWorker(workers.WSConfig{
		Domain:      cfg.Server.Domain,
		ClientToken: cfg.Auth.ClientToken,
	}, stateWorker, authWorker)
	wsWorker.SetDispatcher(dispatcher)

	if err := authWorker.Init(); err != nil {
		alog.Fatal(alog.CatAuth, "初始化 Auth Worker 失败", "error", err)
	}
	alog.Info(alog.CatAuth, "Auth Worker 初始化成功")

	reconcileWorker := workers.NewReconcileWorker(
		workers.DefaultReconcileConfig(),
		stateWorker, controlWorker, dataWorker,
	)

	// ===== Supervisor 启动 =====
	supervisor := NewSupervisor()
	supervisor.Add(authWorker)
	supervisor.Add(stateWorker)
	supervisor.Add(dataWorker)
	supervisor.Add(controlWorker)
	supervisor.Add(wsWorker)
	supervisor.Add(reconcileWorker)
	supervisor.Start()
	alog.Info(alog.CatSystem, "所有 Worker 已启动")

	// ===== 持久化 Provider 注册 =====
	workers.RegisterPersistProviders(stateWorker)

	// ===== 插件初始化 =====
	activePlugins, pluginActions := initPlugins(cfg, stateWorker, dispatcher)
	alog.Info(alog.CatSystem, "插件系统已启动", "count", len(activePlugins))

	statusCollector.SetPlugins(activePlugins)

	// ===== Action 注册表 =====
	actionRegistry := workers.NewActionRegistry()
	taskManager := workers.NewTaskManager()

	// 注册内置插件的 Action handlers
	for _, pa := range pluginActions {
		actionRegistry.RegisterInternal(pa.PluginID, pa.Action, pa.Handler)
	}
	alog.Info(alog.CatSystem, "Action 实现已注册")

	// ===== gRPC Plugin Service =====
	pluginGRPCServer := plugin.NewPluginGRPCServer(actionRegistry)

	// ===== Reconcile 触发：监听 client ADDED 事件 =====
	dispatcher.SubscribeByType(proto.ResourceTypeClient, func(event proto.GenericEvent) {
		if event.EventType == proto.EventAdded {
			reconcileWorker.Trigger()
		}
	})

	// 初始化各组件状态为 ok
	statusCollector.SetAuthStatus("ok", "")
	statusCollector.SetStateStatus("ok", "")
	statusCollector.SetDataStatus("ok", "")
	statusCollector.SetControlStatus("ok", "")
	statusCollector.SetWSStatus("ok", "")

	// ===== HTTP 路由设置：健康检查 / 状态端点 =====
	r := gin.Default()
	r.Use(gin.Recovery(), gin.Logger())

	r.GET("/PING", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"message": "PANG"}})
	})

	r.GET("/status", func(c *gin.Context) {
		clients := controlWorker.ListClients()
		proxies := controlWorker.ListProxies()
		relays := controlWorker.ListRelays()
		statusCollector.UpdateStats(len(clients), len(proxies), len(relays))
		status := statusCollector.GetStatus()
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": status})
	})

	r.GET("/health", func(c *gin.Context) {
		status := statusCollector.GetStatus()
		allOk := status.Components.Auth.Status == "ok" &&
			status.Components.State.Status == "ok" &&
			status.Components.Data.Status == "ok" &&
			status.Components.Control.Status == "ok" &&
			status.Components.WS.Status == "ok"
		if allOk {
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "ok"})
		} else {
			c.JSON(http.StatusServiceUnavailable, gin.H{"code": 503, "msg": "service unavailable", "data": status.Components})
		}
	})

	// ===== 插件同步与执行端点 =====
	r.GET("/api/v1/sync", func(c *gin.Context) {
		grouped := actionRegistry.ListGrouped()
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"plugins": grouped}})
	})

	r.POST("/api/v1/run", func(c *gin.Context) {
		var actionName string
		var params map[string]any

		contentType := c.ContentType()

		if strings.HasPrefix(contentType, "multipart/form-data") {
			// Multipart 模式：action 从 query 读取，body 保留给 handler
			actionName = c.Query("action")
			if actionName == "" {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "action query param required"})
				return
			}
			params = make(map[string]any)
			for k, v := range c.Request.URL.Query() {
				if k != "action" && len(v) > 0 {
					params[k] = v[0]
				}
			}
		} else {
			// JSON 模式（原有逻辑）
			var req struct {
				Action string         `json:"action"`
				Params map[string]any `json:"params"`
				Async  bool           `json:"async,omitempty"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			actionName = req.Action
			params = req.Params

			// 设置参数到 query string
			q := c.Request.URL.Query()
			for k, v := range params {
				q.Set(k, fmt.Sprintf("%v", v))
			}
			c.Request.URL.RawQuery = q.Encode()
		}

		action, ok := actionRegistry.Get(actionName)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "action not found: " + actionName})
			return
		}

		// 同步执行
		if action.Source == workers.ActionSourceInternal && action.Handler != nil {
			action.Handler(c)
		} else if action.Source == workers.ActionSourceExternal {
			// 调用外部插件
			result, err := pluginGRPCServer.ExecuteAction(action.PluginID, actionName, params)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": result})
		} else {
			c.JSON(http.StatusNotImplemented, gin.H{"code": 501, "msg": "action not available"})
		}
	})

	r.GET("/api/v1/tasks/:id", func(c *gin.Context) {
		taskID := c.Param("id")
		task, ok := taskManager.Get(taskID)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "task not found"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": task})
	})

	// ===== 认证路由（登录） =====
	authGroup := r.Group("/api/v1/auth")
	{
		authGroup.POST("/login", func(c *gin.Context) {
			var req struct {
				APIKey string `json:"api_key" binding:"required"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			if !authWorker.ValidateAPIKey(req.APIKey) {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid API key"})
				return
			}
			token, err := authWorker.GenerateToken(req.APIKey)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"token": token, "expires_in": 365 * 24 * 3600}})
		})
	}

	// ===== 注册路由（申请注册、列表、审批、吊销、待审批） =====
	registerPublic := r.Group("/api/v1/register")
	{
		registerPublic.POST("/apply", func(c *gin.Context) {
			var req struct {
				ClientID  string `json:"client_id" binding:"required"`
				PublicKey string `json:"public_key" binding:"required"`
				Token     string `json:"token" binding:"required"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			if !authWorker.ValidateClientToken(req.Token) {
				c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid token"})
				return
			}
			if err := authWorker.AddApplication(req.ClientID, req.PublicKey); err != nil {
				c.JSON(http.StatusConflict, gin.H{"code": 409, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"message": "application submitted", "client_id": req.ClientID}})
		})

		registerPublic.GET("/list", func(c *gin.Context) {
			records := authWorker.GetApproved()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"clients": records}})
		})
	}

	// ===== 受保护的 API 路由（需 JWT 认证）：节点、代理、中继、命名空间 =====
	api := r.Group("/api/v1")
	api.Use(middleware.NewAuthMiddleware(authWorker).RequireJWT())
	{
		// 节点管理
		api.GET("/nodes", func(c *gin.Context) {
			clients := controlWorker.ListClients()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"clients": clients}})
		})

		api.GET("/nodes/:id", func(c *gin.Context) {
			clientID := c.Param("id")
			info, err := controlWorker.GetClientInfo(clientID)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": info})
		})

		// 注册审批
		api.POST("/register/approve", func(c *gin.Context) {
			var req struct {
				ClientID  string `json:"client_id" binding:"required"`
				Namespace string `json:"namespace"`
				Role      string `json:"role"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			if req.Namespace == "" {
				req.Namespace = "permanent"
			}
			if req.Role == "" {
				req.Role = "permanent"
			}
			certPEM, err := authWorker.Approve(req.ClientID, req.Namespace, req.Role)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": err.Error()})
				return
			}
			certStr := string(certPEM)
			certPrefix := certStr
			if len(certStr) > 40 {
				certPrefix = certStr[:40]
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"certificate": certStr, "cert_prefix": certPrefix, "client_id": req.ClientID}})
		})

		// 注册吊销
		api.POST("/register/revoke", func(c *gin.Context) {
			var req struct {
				ClientID string `json:"client_id" binding:"required"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			if authWorker.Delete(req.ClientID) {
				c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"message": "client removed"}})
			} else {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": "client not found"})
			}
		})

		// 待审批列表
		api.GET("/register/pending", func(c *gin.Context) {
			records := authWorker.GetPending()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"applications": records}})
		})

		// 代理管理
		api.GET("/proxies", func(c *gin.Context) {
			proxies := controlWorker.ListProxies()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"proxies": proxies}})
		})

		api.POST("/proxies", func(c *gin.Context) {
			var req workers.CreateProxyRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request: " + err.Error()})
				return
			}
			resp, err := controlWorker.CreateProxy(req)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": resp})
		})

		api.DELETE("/proxies/:port", func(c *gin.Context) {
			portStr := c.Param("port")
			var port int
			fmt.Sscanf(portStr, "%d", &port)
			if err := controlWorker.CloseProxy(port); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"message": "proxy closed"}})
		})

		// 中继管理
		api.POST("/relay", func(c *gin.Context) {
			var req workers.CreateRelayRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request: " + err.Error()})
				return
			}
			resp, err := controlWorker.CreateRelay(req)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": resp})
		})

		api.GET("/relay", func(c *gin.Context) {
			sessions := controlWorker.ListRelays()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"sessions": sessions}})
		})

		api.DELETE("/relay/:id", func(c *gin.Context) {
			sessionID := c.Param("id")
			if err := controlWorker.CloseRelay(sessionID); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"code": 500, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"session_id": sessionID}})
		})

		// 命名空间管理
		api.GET("/namespaces", func(c *gin.Context) {
			namespaces := controlWorker.ListNamespaces()
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"namespaces": namespaces}})
		})

		api.GET("/namespaces/:name", func(c *gin.Context) {
			name := c.Param("name")
			ns, err := controlWorker.GetNamespace(name)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"code": 404, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": ns})
		})

		api.GET("/namespaces/:name/clients", func(c *gin.Context) {
			name := c.Param("name")
			clients := controlWorker.GetNamespaceClients(name)
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"namespace": name, "clients": clients}})
		})

		api.POST("/namespaces/assign", func(c *gin.Context) {
			var req struct {
				ClientID  string `json:"client_id" binding:"required"`
				Namespace string `json:"namespace" binding:"required"`
				Role      string `json:"role" binding:"required"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": "invalid request"})
				return
			}
			if err := controlWorker.SetClientNamespace(req.ClientID, req.Namespace, req.Role); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"code": 400, "msg": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"code": 0, "msg": "success", "data": gin.H{"message": "client namespace updated", "client_id": req.ClientID, "namespace": req.Namespace, "role": req.Role}})
		})
	}

	// ===== WebSocket 端点 =====
	r.GET("/ws", func(c *gin.Context) { wsWorker.Handle(c.Writer, c.Request) })
	r.GET("/ws/temp", func(c *gin.Context) { wsWorker.HandleTemp(c.Writer, c.Request) })
	r.GET("/tunnel", func(c *gin.Context) { wsWorker.HandleTunnel(c.Writer, c.Request) })
	r.GET("/relay", func(c *gin.Context) { wsWorker.HandleRelay(c.Writer, c.Request) })

	// ===== 隧道监听器设置（TCP + KCP/UDP） =====
	if cfg.Server.TunnelPort > 0 {
		// TCP 隧道监听
		go func() {
			ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.TunnelPort))
			if err != nil {
				statusCollector.SetTunnelStatus("error", err.Error())
				alog.Fatal(alog.CatTunnel, "启动 TCP 隧道监听器失败", "error", err)
			}
			defer ln.Close()
			statusCollector.SetTunnelStatus("ok", "")
			alog.Info(alog.CatTunnel, "TCP 隧道监听器已启动", "port", cfg.Server.TunnelPort)
			for {
				conn, err := ln.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						break
					}
					alog.Error(alog.CatTunnel, "TCP 隧道接受错误", "error", err)
					continue
				}
				go handleTunnelConn(conn, dataWorker)
			}
		}()

		// KCP/UDP 隧道监听
		go func() {
			udpConfig := workers.DefaultUDPConfig(cfg.UDPTunnelKey)
			dataWorker.StartKCPProxy(cfg.Server.TunnelPort, "", udpConfig)
		}()
	}

	// ===== gRPC 监听器设置 =====
	var grpcServer *grpc.Server
	if cfg.Server.GRPCPort > 0 {
		grpcServer = grpc.NewServer()
		pluginpb.RegisterPluginServiceServer(grpcServer, pluginGRPCServer)
		go func() {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.Server.GRPCPort))
			if err != nil {
				alog.Fatal(alog.CatSystem, "启动 gRPC 监听器失败", "error", err)
			}
			alog.Info(alog.CatSystem, "gRPC Plugin Service 已启动", "port", cfg.Server.GRPCPort)
			if err := grpcServer.Serve(ln); err != nil {
				alog.Error(alog.CatSystem, "gRPC 服务错误", "error", err)
			}
		}()
	}

	// 将 Dispatcher 事件转发给外部插件
	dispatcher.SubscribeByType("*", func(event proto.GenericEvent) {
		pluginGRPCServer.BroadcastEvent(event)
	})

	// ===== TLS / HTTP 服务器启动 =====
	caCert := authWorker.GetCA()
	if caCert == nil {
		alog.Fatal(alog.CatAuth, "CA证书未加载")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AddCert(caCert)

	server := &http.Server{
		Addr:    cfg.Server.Addr,
		Handler: r,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ClientCAs:  caCertPool,
			ClientAuth: tls.RequestClientCert,
		},
	}

	if cfg.TLS.Enabled {
		alog.Info(alog.CatServer, "正在启动HTTPS/WSS服务器（mTLS）", "addr", server.Addr)
		go func() {
			if err := server.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile); err != nil && err != http.ErrServerClosed {
				alog.Fatal(alog.CatServer, "服务器错误", "error", err)
			}
		}()
	} else {
		alog.Info(alog.CatServer, "正在启动HTTP/WS服务器", "addr", server.Addr)
		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				alog.Fatal(alog.CatServer, "服务器错误", "error", err)
			}
		}()
	}

	// ===== 优雅关闭 =====
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	alog.Info(alog.CatServer, "收到信号，正在优雅关闭", "signal", sig)

	server.Close()
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
	for _, p := range activePlugins {
		p.Stop()
	}
	supervisor.Stop()
	dispatcher.Close()

	alog.Info(alog.CatServer, "服务已停止")
	alog.Flush()
}

// handleTunnelConn 处理新的 TCP 隧道连接
// 先进行认证，认证通过后将连接交给 DataWorker 处理
func handleTunnelConn(conn net.Conn, data *workers.DataWorker) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "handleTunnelConn panic", "error", r)
		}
	}()
	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatTunnel, "新的隧道连接", "remote", remoteAddr)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	token, err := proto.ReadTunnelAuth(conn)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		alog.Error(alog.CatAuth, "隧道认证失败", "remote", remoteAddr, "error", err)
		conn.Close()
		return
	}
	data.AcceptTunnel(conn, token)
}

// PluginAction 内置插件的 Action 定义
type PluginAction struct {
	Action   plugin.Action
	Handler  gin.HandlerFunc
	PluginID string
}

// initPlugins 初始化所有已启用的插件
// 遍历已注册的插件工厂，初始化插件、注册事件监听和 Action handlers
func initPlugins(cfg *config.ServerConfig, state *workers.StateWorker, dispatcher *Dispatcher) ([]plugin.Plugin, []PluginAction) {
	var activePlugins []plugin.Plugin
	var pluginActions []PluginAction
	factories := plugin.GetAll()

	for name, factory := range factories {
		pluginCfg, exists := cfg.Plugins[name]
		if !exists || !pluginCfg.Enabled {
			alog.Info(alog.CatSystem, "插件未启用，跳过", "name", name)
			continue
		}

		p := factory()
		if err := p.Init(dispatcher, state, pluginCfg.Config); err != nil {
			alog.Error(alog.CatSystem, "插件初始化失败", "name", name, "error", err)
			continue
		}

		// 自动注册 Capability: 通过 ResourceHandler 接口
		type ResourceHandler interface {
			HandleResource(event proto.GenericEvent)
		}
		if rh, ok := p.(ResourceHandler); ok {
			for _, cap := range p.Capabilities() {
				handler := func(event proto.GenericEvent) {
					if len(cap.EventTypes) > 0 {
						matched := false
						for _, t := range cap.EventTypes {
							if t == event.EventType {
								matched = true
								break
							}
						}
						if !matched {
							return
						}
					}
					rh.HandleResource(event)
				}
				dispatcher.SubscribeByType(cap.ResourceType, handler)
				alog.Info(alog.CatSystem, "注册插件能力", "plugin", name, "resourceType", cap.ResourceType, "eventTypes", cap.EventTypes)
			}
		}

		// 自动收集 Action handlers（通过 HTTPHandler 接口）
		type HTTPActionHandler interface {
			GetHTTPHandlers() map[string]http.HandlerFunc
		}
		if hah, ok := p.(HTTPActionHandler); ok {
			// 先从插件的 Actions() 获取完整定义（含 Description）
			actionMap := make(map[string]plugin.Action)
			for _, a := range p.Actions() {
				actionMap[a.Name] = a
			}

			for actionName, handler := range hah.GetHTTPHandlers() {
				action := actionMap[actionName]
				if action.Name == "" {
					action = plugin.Action{Name: actionName}
				}
				pluginActions = append(pluginActions, PluginAction{
					Action:   action,
					Handler:  func(c *gin.Context) { handler(c.Writer, c.Request) },
					PluginID: name,
				})
			}
		}

		// 在独立 goroutine 中运行插件
		go func(plug plugin.Plugin) {
			defer func() {
				if r := recover(); r != nil {
					alog.Error(alog.CatSystem, "插件运行崩溃", "name", plug.Name(), "error", r)
				}
			}()
			if err := plug.Run(); err != nil {
				alog.Error(alog.CatSystem, "插件运行出错", "name", plug.Name(), "error", err)
			}
		}(p)

		activePlugins = append(activePlugins, p)
		alog.Info(alog.CatSystem, "插件已启动", "name", name, "version", p.Version())
	}

	return activePlugins, pluginActions
}
