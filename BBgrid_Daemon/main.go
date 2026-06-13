package main

import (
	"BBgrid/BBgrid_Daemon/internal/daemon"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// DaemonConfig daemon 配置
type DaemonConfig struct {
	SocketPath string        `json:"socket_path"`
	LogPath    string        `json:"log_path"`
	CtlPath    string        `json:"ctl_path"`
	UpdateURL  string        `json:"update_url,omitempty"`
	Server     ServiceConfig `json:"server"`
	Client     ServiceConfig `json:"client"`
}

// ServiceConfig 服务配置
type ServiceConfig struct {
	Enabled    bool   `json:"enabled"`
	BinPath    string `json:"bin_path"`
	ConfigPath string `json:"config_path"`
}

func main() {
	configPath := flag.String("config", "", "配置文件路径 (必须)")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("bbgrid-daemon %s (built: %s, commit: %s)\n", Version, BuildTime, GitCommit)
		os.Exit(0)
	}

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: -config is required")
		fmt.Fprintln(os.Stderr, "Usage: bbgrid-daemon -config /path/to/daemon.json")
		os.Exit(1)
	}

	// 加载配置
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Load config failed: %v", err)
	}

	// 创建 daemon
	d := daemon.New(daemon.Config{
		Version:    Version,
		SocketPath: cfg.SocketPath,
		CtlPath:    cfg.CtlPath,
		UpdateURL:  cfg.UpdateURL,
		Server: daemon.ServiceConfig{
			Enabled:    cfg.Server.Enabled,
			BinPath:    cfg.Server.BinPath,
			ConfigPath: cfg.Server.ConfigPath,
		},
		Client: daemon.ServiceConfig{
			Enabled:    cfg.Client.Enabled,
			BinPath:    cfg.Client.BinPath,
			ConfigPath: cfg.Client.ConfigPath,
		},
	})

	// 启动 daemon
	if err := d.Start(); err != nil {
		log.Fatalf("Start daemon failed: %v", err)
	}

	log.Printf("BBgrid Daemon %s started", Version)
	log.Printf("Socket: %s", cfg.SocketPath)

	// 通过 ctl 拉起服务
	d.StartServicesViaCtl()

	// 等待信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("Received signal: %v, shutting down...", sig)
	d.Stop()
	log.Println("Daemon stopped")
}

// loadConfig 加载配置
func loadConfig(path string) (*DaemonConfig, error) {
	baseDir := getBaseDir()

	// 默认配置
	cfg := &DaemonConfig{
		SocketPath: "/var/run/bbgrid/daemon.sock",
		LogPath:    "/var/log/bbgrid/daemon.log",
		CtlPath:    filepath.Join(baseDir, "bin", "bbgrid-ctl"),
		Server: ServiceConfig{
			Enabled:    true,
			BinPath:    filepath.Join(baseDir, "bin", "bbgrid-server"),
			ConfigPath: filepath.Join(baseDir, "config", "server.json"),
		},
		Client: ServiceConfig{
			Enabled:    false,
			BinPath:    filepath.Join(baseDir, "bin", "bbgrid-client"),
			ConfigPath: filepath.Join(baseDir, "config", "client.json"),
		},
	}

	// 加载配置文件
	var configFilePath string
	if path != "" {
		configFilePath = path
	} else {
		// 尝试默认配置文件
		defaultPaths := []string{
			filepath.Join(baseDir, "config", "daemon.json"),
			"/etc/bbgrid/daemon.json",
		}
		for _, p := range defaultPaths {
			if _, err := os.Stat(p); err == nil {
				configFilePath = p
				break
			}
		}
	}

	if configFilePath != "" {
		data, err := os.ReadFile(configFilePath)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
		log.Printf("Loaded config: %s", configFilePath)
	}

	// 确保目录存在
	os.MkdirAll(filepath.Dir(cfg.SocketPath), 0755)
	os.MkdirAll(filepath.Dir(cfg.LogPath), 0755)

	return cfg, nil
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	execPath, err := os.Executable()
	if err != nil {
		return "/usr/local/bbgrid"
	}
	return filepath.Dir(filepath.Dir(execPath))
}
