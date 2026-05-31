package main

import (
	"BBgrid/BBgrid_Server/auth"
	"BBgrid/BBgrid_Server/dataplane"
	ahttp "BBgrid/BBgrid_Server/http"
	"BBgrid/BBgrid_Server/runtime"
	"BBgrid/BBgrid_Server/session"
	alog "BBgrid/common/log"
	"BBgrid/common/store"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	// 内置插件
	"BBgrid/plugins/file"
	"BBgrid/plugins/latency"
	"BBgrid/plugins/persist"
	proxyPlugin "BBgrid/plugins/proxy"
	relayPlugin "BBgrid/plugins/relay"
	"BBgrid/plugins/tag"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {
	configPath := flag.String("config", "config.json", "配置文件路径")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("Aether Server %s (built: %s, commit: %s)\n", Version, BuildTime, GitCommit)
		os.Exit(0)
	}

	// 加载配置
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}

	// 配置日志文件
	if cfg.LogPath != "" {
		if err := alog.SetFile(cfg.LogPath); err != nil {
			fmt.Fprintf(os.Stderr, "设置日志文件失败: %v\n", err)
		} else {
			alog.Info(alog.CatSystem, "日志文件已启用", "path", cfg.LogPath)
		}
	}

	alog.Info(alog.CatSystem, "Aether Server 启动",
		"version", Version,
		"addr", cfg.Addr,
		"tunnel_port", cfg.TunnelPort,
	)

	// 初始化存储
	storage, err := store.NewStorageManager(store.StorageConfig{
		DataDir:          cfg.DataDir,
		SnapshotInterval: 1000,
	})
	if err != nil {
		alog.Fatal(alog.CatSystem, "初始化存储失败", "error", err)
	}

	// 初始化 Auth 模块
	authManager := auth.NewManager(auth.Config{
		DataDir:     cfg.DataDir,
		APIKey:      cfg.APIKey,
		ClientToken: cfg.ClientToken,
	})
	if err := authManager.Init(); err != nil {
		alog.Fatal(alog.CatSystem, "初始化 Auth 失败", "error", err)
	}

	// 创建 Runtime Core
	core := runtime.NewCore(runtime.CoreConfig{
		PublicIP:          cfg.PublicIP,
		ReconcileInterval: 30,
	}, storage)

	// 创建 Session Layer（先订阅 EventBus，再启动插件恢复）
	sess := session.NewServer(core, cfg.Domain, cfg.TunnelPort, authManager)

	// 初始化插件（插件 Run() 中的恢复事件需要 session handler 已订阅）
	plugins := initPlugins(core, cfg)

	// 插件初始化完成，启动 Session Layer 的 EventBus 订阅
	sess.StartEventSubscriptions()

	// 创建 Data Plane
	dataplane := dataplane.NewServer(cfg.TunnelPort)

	// 连接 Data Plane 和 Session (tunnel 连接配对)
	dataplane.SetTunnelHandler(func(token string, conn net.Conn) {
		sess.AcceptTunnel(token, conn)
	})

	httpServer := ahttp.NewServer(core, sess, authManager, Version)
	httpServer.SetupRoutes()

	// 启动 Runtime Core
	core.Start()

	// 启动 Data Plane (goroutine)
	go func() {
		if err := dataplane.Run(); err != nil {
			alog.Error(alog.CatSystem, "Data Plane 异常退出", "error", err)
		}
	}()

	// 启动 HTTP Control Plane (goroutine)
	go func() {
		var err error
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			// 创建 TLS 配置 (请求客户端证书但不验证)
			tlsConfig := &tls.Config{
				ClientAuth: tls.RequestClientCert,
			}

			// 如果有 CA 证书，把它发给客户端作为可接受 CA 提示。
			// 身份校验放在 Session Layer 的 register 流程里完成，避免 TLS 层
			// 因旧证书或代理链路缺失客户端证书而直接拒绝连接。
			caCertPath := cfg.DataDir + "/ca.crt"
			if caCert, err := os.ReadFile(caCertPath); err == nil {
				caCertPool := x509.NewCertPool()
				if caCertPool.AppendCertsFromPEM(caCert) {
					tlsConfig.ClientCAs = caCertPool
					alog.Info(alog.CatSystem, "已加载 CA 证书", "path", caCertPath)
				}
			}

			err = httpServer.RunTLS(cfg.Addr, cfg.TLSCert, cfg.TLSKey, tlsConfig)
		} else {
			err = httpServer.Run(cfg.Addr)
		}
		if err != nil {
			alog.Fatal(alog.CatSystem, "HTTP 服务器异常退出", "error", err)
		}
	}()

	alog.Info(alog.CatSystem, "Aether Server 就绪",
		"control_plane", cfg.Addr,
		"data_plane", fmt.Sprintf(":%d", cfg.TunnelPort),
	)

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	alog.Info(alog.CatSystem, "收到关闭信号", "signal", sig)

	// 优雅关闭
	sess.Stop()
	dataplane.Stop()
	for _, plugin := range plugins {
		plugin.Stop()
	}
	core.Stop()
	storage.Close()
	alog.Flush()

	alog.Info(alog.CatSystem, "Aether Server 已关闭")
}

// initPlugins 初始化所有内置插件
func initPlugins(core *runtime.Core, cfg *Config) []pluginImpl {
	pluginConfig := map[string]any{
		"data_dir": cfg.DataDir,
	}
	running := make([]pluginImpl, 0, len(cfg.Plugins))

	// 初始化插件列表
	plugins := []struct {
		name    string
		factory func() pluginImpl
	}{
		{"latency", func() pluginImpl { return latency.New() }},
		{"persist", func() pluginImpl { return persist.New() }},
		{"tag", func() pluginImpl { return tag.New() }},
		{"file", func() pluginImpl { return file.New() }},
		{"proxy-provider", func() pluginImpl { return proxyPlugin.New() }},
		{"relay-provider", func() pluginImpl { return relayPlugin.New() }},
	}

	for _, p := range plugins {
		// 检查插件是否启用 (默认启用)
		enabled := true
		var pluginSpecific map[string]any
		if cfg.Plugins != nil {
			if pc, ok := cfg.Plugins[p.name]; ok {
				enabled = pc.Enabled
				pluginSpecific = pc.Config
			}
		}

		if !enabled {
			alog.Info(alog.CatSystem, "插件已禁用", "name", p.name)
			continue
		}

		// 合并全局配置 + 插件专属配置（插件专属优先）
		finalConfig := make(map[string]any, len(pluginConfig)+len(pluginSpecific))
		for k, v := range pluginConfig {
			finalConfig[k] = v
		}
		for k, v := range pluginSpecific {
			finalConfig[k] = v
		}

		plugin := p.factory()
		if err := plugin.Init(core, finalConfig); err != nil {
			alog.Error(alog.CatSystem, "插件初始化失败", "name", p.name, "error", err)
			continue
		}

		// 启动插件 (goroutine)
		go func(name string, p pluginImpl) {
			if err := p.Run(); err != nil {
				alog.Error(alog.CatSystem, "插件异常退出", "name", name, "error", err)
			}
		}(p.name, plugin)

		running = append(running, plugin)
		alog.Info(alog.CatSystem, "插件已启动", "name", p.name)
	}

	return running
}

// pluginImpl 插件接口 (简化版)
type pluginImpl interface {
	Init(core *runtime.Core, config map[string]any) error
	Run() error
	Stop()
}

// ==================== Config ====================

type Config struct {
	Addr        string                  `json:"addr"`
	Domain      string                  `json:"domain"`
	TunnelPort  int                     `json:"tunnel_port"`
	PublicIP    string                  `json:"public_ip"`
	DataDir     string                  `json:"data_dir"`
	LogPath     string                  `json:"log_path"`
	APIKey      string                  `json:"api_key"`
	ClientToken string                  `json:"client_token"`
	TLSCert     string                  `json:"tls_cert"`
	TLSKey      string                  `json:"tls_key"`
	Plugins     map[string]PluginConfig `json:"plugins"`
}

type PluginConfig struct {
	Enabled bool           `json:"enabled"`
	Config  map[string]any `json:"config"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Addr:       ":9909",
		TunnelPort: 9908,
		DataDir:    "data",
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	if cfg.APIKey == "" {
		return nil, fmt.Errorf("api_key is required")
	}
	if cfg.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required")
	}

	return cfg, nil
}
