package config

import (
	"fmt"
	"os"
)

type ServerConfig struct {
	Server       ServerSettings          `json:"server"`
	TLS          TLSSettings             `json:"tls"`
	Auth         AuthSettings            `json:"auth"`
	Storage      string                  `json:"storage"`
	DataDir      string                  `json:"data_dir"`
	LogPath      string                  `json:"log_path"`
	PublicIP     string                  `json:"public_ip"`
	Plugins      map[string]PluginConfig `json:"plugins"`
	UDPTunnelKey string                  `json:"udp_tunnel_key"`
}

type ServerSettings struct {
	Addr       string `json:"addr"`
	Domain     string `json:"domain"`
	TunnelPort int    `json:"tunnel_port"`
	GRPCPort   int    `json:"grpc_port"`
}

type TLSSettings struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

type AuthSettings struct {
	APIKey      string `json:"api_key"`
	ClientToken string `json:"client_token"`
}

type PluginConfig struct {
	Enabled bool           `json:"enabled"`
	Config  map[string]any `json:"config"`
}

func defaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Server: ServerSettings{
			Addr:       ":9909",
			TunnelPort: 9908,
		},
		TLS: TLSSettings{
			Enabled:  true,
			CertFile: "ssl/cert.pem",
			KeyFile:  "ssl/key.pem",
		},
		Storage: "data/proxies.json",
		DataDir: "data",
	}
}

func LoadServer(path string) (*ServerConfig, error) {
	cfg := defaultServerConfig()

	if path != "" {
		if err := loadJSON(path, cfg); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	// server settings
	cfg.Server.Addr = envStr("BBGRID_SERVER_ADDR", cfg.Server.Addr)
	cfg.Server.Domain = envStr("BBGRID_DOMAIN", cfg.Server.Domain)
	cfg.Server.TunnelPort = envInt("BBGRID_TUNNEL_PORT", cfg.Server.TunnelPort)

	// tls settings
	cfg.TLS.CertFile = envStr("BBGRID_TLS_CERT", cfg.TLS.CertFile)
	cfg.TLS.KeyFile = envStr("BBGRID_TLS_KEY", cfg.TLS.KeyFile)
	if v := os.Getenv("BBGRID_TLS_ENABLED"); v != "" {
		cfg.TLS.Enabled = v == "true" || v == "1"
	}

	// auth settings
	cfg.Auth.APIKey = envStr("BBGRID_API_KEY", cfg.Auth.APIKey)
	cfg.Auth.ClientToken = envStr("BBGRID_CLIENT_TOKEN", cfg.Auth.ClientToken)

	// other
	cfg.Storage = envStr("BBGRID_STORAGE", cfg.Storage)
	cfg.DataDir = envStr("BBGRID_DATA_DIR", cfg.DataDir)
	cfg.LogPath = envStr("BBGRID_LOG_PATH", cfg.LogPath)
	cfg.PublicIP = envStr("BBGRID_PUBLIC_IP", cfg.PublicIP)
	cfg.UDPTunnelKey = envStr("BBGRID_UDP_TUNNEL_KEY", cfg.UDPTunnelKey)

	// validate
	if cfg.Auth.APIKey == "" {
		return nil, fmt.Errorf("api_key is required (set in config file or BBGRID_API_KEY env)")
	}
	if cfg.Auth.ClientToken == "" {
		return nil, fmt.Errorf("client_token is required (set in config file or BBGRID_CLIENT_TOKEN env)")
	}

	return cfg, nil
}
