// Package auth 提供认证管理抽象。
package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// AuthMode 认证模式
type AuthMode string

const (
	AuthModeMTLS  AuthMode = "mtls"  // mTLS 双向认证
	AuthModeToken AuthMode = "token" // Token 认证
)

// Config 认证配置
type Config struct {
	Mode           AuthMode `json:"mode"`
	PrivateKeyPath string   `json:"private_key_path"`
	CertPath       string   `json:"cert_path"`
	CACertPath     string   `json:"ca_cert_path"`
	Token          string   `json:"token"`
	Insecure       bool     `json:"insecure"`
}

// Manager 认证管理器
type Manager struct {
	config    Config
	tlsConfig *tls.Config
}

// NewManager 创建认证管理器
func NewManager(config Config) *Manager {
	return &Manager{
		config: config,
	}
}

// Init 初始化认证
func (m *Manager) Init() error {
	switch m.config.Mode {
	case AuthModeMTLS:
		return m.initMTLS()
	case AuthModeToken:
		return m.initToken()
	default:
		return fmt.Errorf("unknown auth mode: %s", m.config.Mode)
	}
}

// initMTLS 初始化 mTLS 认证
func (m *Manager) initMTLS() error {
	if m.config.CertPath == "" || m.config.PrivateKeyPath == "" {
		return fmt.Errorf("mTLS requires cert_path and private_key_path")
	}

	// 加载客户端证书和私钥
	cert, err := tls.LoadX509KeyPair(m.config.CertPath, m.config.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("load key pair: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: m.config.Insecure,
		MinVersion:         tls.VersionTLS12,
	}

	// 加载 CA 证书（用于验证服务器证书）
	if m.config.CACertPath != "" {
		caCert, err := os.ReadFile(m.config.CACertPath)
		if err != nil {
			return fmt.Errorf("load CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to parse CA cert")
		}
		tlsConfig.RootCAs = pool
	}

	m.tlsConfig = tlsConfig
	return nil
}

// initToken 初始化 Token 认证
func (m *Manager) initToken() error {
	if m.config.Token == "" {
		return fmt.Errorf("token auth requires token")
	}
	return nil
}

// GetTLSConfig 获取 TLS 配置
func (m *Manager) GetTLSConfig() *tls.Config {
	return m.tlsConfig
}

// GetToken 获取认证 Token
func (m *Manager) GetToken() string {
	return m.config.Token
}

// GetMode 获取认证模式
func (m *Manager) GetMode() AuthMode {
	return m.config.Mode
}

// IsInsecure 是否跳过 TLS 验证
func (m *Manager) IsInsecure() bool {
	return m.config.Insecure
}

// CertExists 检查证书是否存在
func (m *Manager) CertExists() bool {
	if m.config.CertPath == "" {
		return false
	}
	_, err := os.Stat(m.config.CertPath)
	return err == nil
}

// KeyExists 检查私钥是否存在
func (m *Manager) KeyExists() bool {
	if m.config.PrivateKeyPath == "" {
		return false
	}
	_, err := os.Stat(m.config.PrivateKeyPath)
	return err == nil
}
