package config

import (
	"BBgrid/BBgrid_Client/register"
	"fmt"
	"os"
	"path/filepath"
)

// FilesConfig 文件 API 配置
type FilesConfig struct {
	APIEnabled      bool `json:"api_enabled"`
	APIPort         int  `json:"api_port"`
	BatchSizeKB     int  `json:"batch_size_kb"`
	BatchIntervalSec int `json:"batch_interval_sec"`
}

type ClientConfig struct {
	ServerURL             string `json:"server_url"`
	ClientToken           string `json:"client_token"`
	ClientID              string `json:"client_id"`
	Voucher               string `json:"voucher"`
	DataDir               string `json:"data_dir"`
	LogPath               string `json:"log_path"`
	PrivateKeyPath        string `json:"private_key_path"`
	PublicKeyPath         string `json:"public_key_path"`
	CertificatePath       string `json:"certificate_path"`
	UseHTTP               bool   `json:"use_http"`
	Insecure              bool   `json:"insecure"`
	TLSSNI                string `json:"tls_sni"`
	Origin                string `json:"origin"`
	ReconnectDelaySeconds int    `json:"reconnect_delay_seconds"`
	UDPTunnelKey          string `json:"udp_tunnel_key"`
	Files                 FilesConfig `json:"files"`
}

func defaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientID:              "raspberry-pi-01",
		DataDir:               "./data",
		PrivateKeyPath:        "./data/client.key",
		PublicKeyPath:         "./data/client.pub",
		CertificatePath:       "./data/server.crt",
		ReconnectDelaySeconds: 5,
		Files: FilesConfig{
			APIEnabled:       false,
			APIPort:          8080,
			BatchSizeKB:      10240, // 10MB
			BatchIntervalSec: 1800,  // 30分钟
		},
	}
}

func LoadClient(path string) (*ClientConfig, error) {
	cfg := defaultClientConfig()

	if path != "" {
		if err := loadJSON(path, cfg); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	cfg.ServerURL = envStr("BBGRID_WS_URL", cfg.ServerURL)
	cfg.ClientToken = envStr("BBGRID_CLIENT_TOKEN", cfg.ClientToken)
	cfg.ClientID = envStr("BBGRID_CLIENT_ID", cfg.ClientID)
	cfg.Voucher = envStr("BBGRID_VOUCHER", cfg.Voucher)
	cfg.DataDir = envStr("BBGRID_DATA_DIR", cfg.DataDir)
	cfg.LogPath = envStr("BBGRID_LOG_PATH", cfg.LogPath)
	cfg.UseHTTP = envBool("BBGRID_USE_HTTP", cfg.UseHTTP)
	cfg.Insecure = envBool("BBGRID_INSECURE", cfg.Insecure)
	cfg.TLSSNI = envStr("BBGRID_TLS_SNI", cfg.TLSSNI)
	cfg.Origin = envStr("BBGRID_ORIGIN", cfg.Origin)
	cfg.ReconnectDelaySeconds = envInt("BBGRID_RECONNECT_DELAY", cfg.ReconnectDelaySeconds)
	cfg.UDPTunnelKey = envStr("BBGRID_UDP_TUNNEL_KEY", cfg.UDPTunnelKey)
	cfg.Files.APIEnabled = envBool("BBGRID_FILES_API_ENABLED", cfg.Files.APIEnabled)
	cfg.Files.APIPort = envInt("BBGRID_FILES_API_PORT", cfg.Files.APIPort)
	cfg.Files.BatchSizeKB = envInt("BBGRID_FILES_BATCH_SIZE_KB", cfg.Files.BatchSizeKB)
	cfg.Files.BatchIntervalSec = envInt("BBGRID_FILES_BATCH_INTERVAL_SEC", cfg.Files.BatchIntervalSec)

	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("server_url is required (set in config file or BBGRID_WS_URL env)")
	}
	if cfg.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required (set in config file or BBGRID_CLIENT_TOKEN env)")
	}

	// 如果路径是相对路径且未自定义，基于 DataDir 推导
	if cfg.DataDir != "" {
		if cfg.PrivateKeyPath == defaultClientConfig().PrivateKeyPath {
			cfg.PrivateKeyPath = filepath.Join(cfg.DataDir, "client.key")
		}
		if cfg.PublicKeyPath == defaultClientConfig().PublicKeyPath {
			cfg.PublicKeyPath = filepath.Join(cfg.DataDir, "client.pub")
		}
		if cfg.CertificatePath == defaultClientConfig().CertificatePath {
			cfg.CertificatePath = filepath.Join(cfg.DataDir, "server.crt")
		}
	}

	// 创建目录
	dataDir := filepath.Dir(cfg.PrivateKeyPath)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data directory %s: %w", dataDir, err)
	}

	// 检查密钥对是否存在，否则生成
	if !fileExists(cfg.PrivateKeyPath) || !fileExists(cfg.PublicKeyPath) {
		err := register.GenerateKeyPair(cfg.PrivateKeyPath, cfg.PublicKeyPath)
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
