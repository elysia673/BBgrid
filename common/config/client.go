package config

import (
	"BBgrid/BBgrid_Client/register"
	"fmt"
	"os"
	"path/filepath"
)

type ClientConfig struct {
	ServerURL             string `json:"server_url"`
	ClientToken           string `json:"client_token"`
	ClientID              string `json:"client_id"`
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
}

func defaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ClientID:              "raspberry-pi-01",
		DataDir:               "./data",
		PrivateKeyPath:        "./data/client.key",
		PublicKeyPath:         "./data/client.pub",
		CertificatePath:       "./data/server.crt",
		ReconnectDelaySeconds: 5,
	}
}

func LoadClient(path string) (*ClientConfig, error) {
	cfg := defaultClientConfig()

	if path != "" {
		if err := loadJSON(path, cfg); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	cfg.ServerURL = envStr("AETHER_WS_URL", cfg.ServerURL)
	cfg.ClientToken = envStr("AETHER_CLIENT_TOKEN", cfg.ClientToken)
	cfg.ClientID = envStr("AETHER_CLIENT_ID", cfg.ClientID)
	cfg.DataDir = envStr("AETHER_DATA_DIR", cfg.DataDir)
	cfg.LogPath = envStr("AETHER_LOG_PATH", cfg.LogPath)
	cfg.UseHTTP = envBool("AETHER_USE_HTTP", cfg.UseHTTP)
	cfg.Insecure = envBool("AETHER_INSECURE", cfg.Insecure)
	cfg.TLSSNI = envStr("AETHER_TLS_SNI", cfg.TLSSNI)
	cfg.Origin = envStr("AETHER_ORIGIN", cfg.Origin)
	cfg.ReconnectDelaySeconds = envInt("AETHER_RECONNECT_DELAY", cfg.ReconnectDelaySeconds)
	cfg.UDPTunnelKey = envStr("AETHER_UDP_TUNNEL_KEY", cfg.UDPTunnelKey)

	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("server_url is required (set in config file or AETHER_WS_URL env)")
	}
	if cfg.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required (set in config file or AETHER_CLIENT_TOKEN env)")
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
	os.MkdirAll(dataDir, 0755)

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
