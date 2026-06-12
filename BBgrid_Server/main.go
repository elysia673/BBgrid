package main

import (
	"BBgrid/BBgrid_Server/auth"
	"BBgrid/BBgrid_Server/dataplane"
	ahttp "BBgrid/BBgrid_Server/http"
	"BBgrid/BBgrid_Server/plugin"
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

// 默认配置常量
const (
	DefaultHTTPAddr          = ":9909"
	DefaultTunnelPort        = 9908
	DefaultDataDir           = "data"
	DefaultReconcileInterval = 30
	DefaultSnapshotInterval  = 1000
)

func main() {
	configPath := flag.String("config", "config.json", "配置文件路径")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("BBgrid Server %s (built: %s, commit: %s)\n", Version, BuildTime, GitCommit)
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

	alog.Info(alog.CatSystem, "BBgrid Server 启动",
		"version", Version,
		"addr", cfg.Addr,
		"tunnel_port", cfg.TunnelPort,
	)

	// 初始化存储
	storage, err := store.NewStorageManager(store.StorageConfig{
		DataDir:          cfg.DataDir,
		SnapshotInterval: DefaultSnapshotInterval,
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

	// 创建 Runtime Core 事件驱动
	core := runtime.NewCore(runtime.CoreConfig{
		PublicIP:          cfg.PublicIP,
		ReconcileInterval: DefaultReconcileInterval,
	}, storage)

	// 创建 Session Layer
	sess := session.NewServer(core, cfg.Domain, cfg.TunnelPort, authManager)

	// 创建插件管理器
	pluginManager := plugin.NewManager(core, map[string]any{
		"data_dir":    cfg.DataDir,
		"tunnel_port": cfg.TunnelPort,
		"public_ip":   cfg.PublicIP,
	})

	// 注册内置插件
	pluginManager.Register("latency", func() plugin.Plugin { return latency.New() })
	pluginManager.Register("persist", func() plugin.Plugin { return persist.New() })
	pluginManager.Register("tag", func() plugin.Plugin { return tag.New() })
	pluginManager.Register("file", func() plugin.Plugin { return file.New() })
	pluginManager.Register("proxy-provider", func() plugin.Plugin { return proxyPlugin.New() })
	pluginManager.Register("relay-provider", func() plugin.Plugin { return relayPlugin.New() })

	// 转换插件配置格式
	pluginsConfig := convertPluginsConfig(cfg.Plugins)

	// 初始化并启动插件
	pluginManager.InitAll(pluginsConfig)
	pluginManager.StartAll()

	// 注入 notifyFn 到 proxy 和 relay 插件
	if p, ok := pluginManager.Get("proxy-provider"); ok {
		if proxyP, ok := p.(*proxyPlugin.Plugin); ok {
			proxyP.SetNotifyFn(func(clientID string, msg any) error {
				return sess.SendToClient(clientID, msg)
			})
		}
	}
	if p, ok := pluginManager.Get("relay-provider"); ok {
		if relayP, ok := p.(*relayPlugin.Plugin); ok {
			relayP.SetNotifyFn(func(clientID string, msg any) error {
				return sess.SendToClient(clientID, msg)
			})
		}
	}

	// 插件初始化完成，启动 Session Layer 的 EventBus 订阅
	sess.StartEventSubscriptions()

	// 创建 Data Plane
	dataplane := dataplane.NewServer(cfg.TunnelPort)

	// 连接 Data Plane 和 Session
	dataplane.SetTunnelHandler(func(token string, conn net.Conn) {
		sess.AcceptTunnel(token, conn)
	})

	httpServer := ahttp.NewServer(core, sess, authManager, Version)
	httpServer.SetupRoutes()

	// 启动 Runtime Core
	core.Start()

	// 启动 Data Plane
	go func() {
		if err := dataplane.Run(); err != nil {
			alog.Error(alog.CatSystem, "Data Plane 异常退出", "error", err)
		}
	}()

	// 启动 HTTP Control Plane
	go func() {
		var err error
		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			tlsConfig := &tls.Config{
				ClientAuth: tls.RequestClientCert,
			}

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

	alog.Info(alog.CatSystem, "BBgrid Server 就绪",
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
	pluginManager.StopAll()
	core.Stop()
	storage.Close()
	alog.Flush()

	alog.Info(alog.CatSystem, "BBgrid Server 已关闭")
}

// convertPluginsConfig 转换插件配置格式
func convertPluginsConfig(plugins map[string]PluginConfig) map[string]map[string]any {
	if plugins == nil {
		return nil
	}
	result := make(map[string]map[string]any, len(plugins))
	for name, pc := range plugins {
		cfg := make(map[string]any)
		cfg["enabled"] = pc.Enabled
		for k, v := range pc.Config {
			cfg[k] = v
		}
		result[name] = cfg
	}
	return result
}

// ==================== Config ====================

// Config 服务器配置
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

// PluginConfig 插件配置
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
		Addr:       DefaultHTTPAddr,
		TunnelPort: DefaultTunnelPort,
		DataDir:    DefaultDataDir,
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
