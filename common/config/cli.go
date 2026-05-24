package config

import (
	"fmt"
	"os"
	"strings"
)

type CLIConfig struct {
	Server   string `json:"server"`
	APIKey   string `json:"api_key"`
	Insecure bool   `json:"insecure"`
}

func defaultCLIConfig() *CLIConfig {
	return &CLIConfig{}
}

func LoadCLI(path string) (*CLIConfig, error) {
	cfg := defaultCLIConfig()

	if path != "" {
		if err := loadJSON(path, cfg); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}

	cfg.Server = envStr("AETHER_SERVER", cfg.Server)
	cfg.Server = strings.TrimRight(cfg.Server, "/")
	cfg.APIKey = envStr("AETHER_API_KEY", cfg.APIKey)
	cfg.Insecure = envBool("AETHER_INSECURE", cfg.Insecure)

	if cfg.Server == "" {
		return nil, fmt.Errorf("server is required (set in config file or AETHER_SERVER env)")
	}
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("api_key is required (set in config file or AETHER_API_KEY env)")
	}

	return cfg, nil
}
