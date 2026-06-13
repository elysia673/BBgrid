// Package config 提供配置管理功能。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// ServerConfig 服务器配置模板
var ServerConfig = map[string]any{
	"addr":        ":9909",
	"domain":      "",
	"tunnel_port": 9908,
	"public_ip":   "",
	"data_dir":    "./data",
	"log_path":    "./data/server.log",
	"api_key":     "your-api-key-here",
	"client_token": "your-client-token-here",
	"tls_cert":    "",
	"tls_key":     "",
	"plugins": map[string]any{
		"latency": map[string]any{"enabled": true},
		"persist": map[string]any{"enabled": true},
		"tag":     map[string]any{"enabled": true},
		"file":    map[string]any{"enabled": true},
		"proxy-provider": map[string]any{"enabled": true},
		"relay-provider": map[string]any{"enabled": true},
	},
}

// ClientConfig 客户端配置模板
var ClientConfig = map[string]any{
	"server_url":               "wss://your-server:9909/ws",
	"client_id":                "my-device",
	"client_token":             "your-client-token-here",
	"data_dir":                 "./data",
	"log_path":                 "./data/client.log",
	"use_http":                 false,
	"insecure":                 false,
	"tls_sni":                  "",
	"origin":                   "",
	"reconnect_delay_seconds":  5,
}

// Manager 配置管理器
type Manager struct {
	baseDir   string
	configDir string
}

// NewManager 创建配置管理器
func NewManager() *Manager {
	base := getBaseDir()
	return &Manager{
		baseDir:   base,
		configDir: filepath.Join(base, "config"),
	}
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	execPath, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(filepath.Dir(execPath))
}

// Init 生成配置文件
func (m *Manager) Init(target string) error {
	switch target {
	case "server":
		return m.initServer()
	case "client":
		return m.initClient()
	case "all":
		if err := m.initServer(); err != nil {
			return err
		}
		return m.initClient()
	default:
		return fmt.Errorf("unknown target: %s (use server|client|all)", target)
	}
}

// initServer 生成服务器配置
func (m *Manager) initServer() error {
	configPath := filepath.Join(m.configDir, "server.json")

	// 检查是否已存在
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("  Config already exists: %s\n", configPath)
		fmt.Println("  Use --force to overwrite")
		return nil
	}

	// 确保目录存在
	os.MkdirAll(m.configDir, 0755)

	// 写入配置
	data, err := json.MarshalIndent(ServerConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	fmt.Printf("  Server config generated: %s\n", configPath)
	fmt.Println("  Please edit the config file before starting the server")
	return nil
}

// initClient 生成客户端配置
func (m *Manager) initClient() error {
	configPath := filepath.Join(m.configDir, "client.json")

	// 检查是否已存在
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("  Config already exists: %s\n", configPath)
		fmt.Println("  Use --force to overwrite")
		return nil
	}

	// 确保目录存在
	os.MkdirAll(m.configDir, 0755)

	// 写入配置
	data, err := json.MarshalIndent(ClientConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	fmt.Printf("  Client config generated: %s\n", configPath)
	fmt.Println("  Please edit the config file before starting the client")
	return nil
}

// GetConfigPath 获取配置文件路径
func (m *Manager) GetConfigPath(target string) string {
	return filepath.Join(m.configDir, target+".json")
}

// GetBinPath 获取二进制文件路径
func (m *Manager) GetBinPath(target string) string {
	binName := "bbgrid-" + target
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	return filepath.Join(m.baseDir, "bin", binName)
}

// GetLogPath 获取日志文件路径
func (m *Manager) GetLogPath(target string) string {
	return filepath.Join(m.baseDir, "logs", target+".log")
}

// GetDataDir 获取数据目录
func (m *Manager) GetDataDir() string {
	return filepath.Join(m.baseDir, "data")
}
