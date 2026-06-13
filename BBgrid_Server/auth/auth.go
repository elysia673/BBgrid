// Package auth 提供认证和授权功能
//
// 独立模块，不依赖其他 Aether 包。
// 负责:
// - CA 证书管理
// - JWT Token 生成/验证
// - 客户端注册表
// - 命名空间管理
package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ==================== Config ====================

type Config struct {
	DataDir     string
	APIKey      string
	ClientToken string
}

// ==================== Manager ====================

// Manager 认证管理器
type Manager struct {
	config     Config
	caCert     *x509.Certificate
	caKey      *ecdsa.PrivateKey
	caMu       sync.RWMutex
	jwtSecret  []byte
	jwtFile    string
	clients    map[string]*ClientRecord
	namespaces map[string]*NamespaceInfo
	vouchers   map[string]*Voucher
	mu         sync.RWMutex
}

// ClientRecord 客户端记录
type ClientRecord struct {
	ClientID    string `json:"client_id"`
	PublicKey   string `json:"public_key"`
	Certificate string `json:"certificate,omitempty"`
	Status      string `json:"status"` // pending, approved, rejected
	Namespace   string `json:"namespace"`
	Role        string `json:"role"`
	CreatedAt   int64  `json:"created_at"`
	ApprovedAt  int64  `json:"approved_at,omitempty"`
}

// NamespaceInfo 命名空间信息
type NamespaceInfo struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Type        string   `json:"type"`
	Clients     []string `json:"clients"`
	CreatedAt   int64    `json:"created_at"`
}

// Voucher 注册凭证
type Voucher struct {
	Code      string `json:"code"`       // 凭证码
	MaxUses   int    `json:"max_uses"`   // 最大使用次数 (0=不限)
	UsedCount int    `json:"used_count"` // 已使用次数
	ExpiresAt int64  `json:"expires_at"` // 过期时间戳 (0=不过期)
	Namespace string `json:"namespace"`  // 注册到哪个命名空间
	Role      string `json:"role"`       // 默认角色
	CreatedAt int64  `json:"created_at"`
}

// JWTClaims JWT 声明
type JWTClaims struct {
	APIKey string `json:"api_key"`
	jwt.RegisteredClaims
}

// NewManager 创建认证管理器
func NewManager(config Config) *Manager {
	return &Manager{
		config:     config,
		clients:    make(map[string]*ClientRecord),
		namespaces: make(map[string]*NamespaceInfo),
		vouchers:   make(map[string]*Voucher),
		jwtFile:    filepath.Join(config.DataDir, "jwt.secret"),
	}
}

// Init 初始化认证管理器
func (m *Manager) Init() error {
	// 初始化 CA
	if err := m.initCA(); err != nil {
		return fmt.Errorf("init CA: %w", err)
	}

	// 初始化 JWT
	if err := m.initJWT(); err != nil {
		return fmt.Errorf("init JWT: %w", err)
	}

	// 加载客户端注册表
	m.loadRegistry()

	// 加载凭证
	m.loadVouchers()

	// 初始化默认命名空间
	m.initDefaultNamespaces()

	return nil
}

// ==================== CA ====================

func (m *Manager) initCA() error {
	certPath := filepath.Join(m.config.DataDir, "ca.crt")
	keyPath := filepath.Join(m.config.DataDir, "ca.key")

	if fileExists(certPath) && fileExists(keyPath) {
		return m.loadCA(certPath, keyPath)
	}
	return m.generateCA(certPath, keyPath)
}

func (m *Manager) loadCA(certPath, keyPath string) error {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return err
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return fmt.Errorf("invalid CA cert PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}

	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return fmt.Errorf("invalid CA key PEM")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return err
	}

	m.caCert = cert
	m.caKey = key
	return nil
}

func (m *Manager) generateCA(certPath, keyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Aether CA",
			Organization: []string{"Aether"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour * 10),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return err
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return err
	}

	// 保存证书
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return fmt.Errorf("save CA cert: %w", err)
	}

	// 保存私钥
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("save CA key: %w", err)
	}

	m.caCert = cert
	m.caKey = key
	return nil
}

// GetCA 获取 CA 证书
func (m *Manager) GetCA() *x509.Certificate {
	m.caMu.RLock()
	defer m.caMu.RUnlock()
	return m.caCert
}

// GetCACert 获取 CA 证书 PEM 格式
func (m *Manager) GetCACert() []byte {
	m.caMu.RLock()
	defer m.caMu.RUnlock()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: m.caCert.Raw})
}

// GetClientCertificate 获取客户端证书（已签发的证书，不论当前状态）
func (m *Manager) GetClientCertificate(clientID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, exists := m.clients[clientID]
	if !exists || client.Certificate == "" {
		return "", false
	}
	return client.Certificate, true
}

// ClientKeyChanged 检查客户端公钥是否与存储的不同
func (m *Manager) ClientKeyChanged(clientID, publicKey string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, exists := m.clients[clientID]
	if !exists {
		return true
	}
	return client.PublicKey != publicKey
}

// SignClientCertificate 签名客户端证书
func (m *Manager) SignClientCertificate(clientPublicKeyPEM []byte, clientID string, validityDays int) ([]byte, error) {
	m.caMu.Lock()
	defer m.caMu.Unlock()

	block, _ := pem.Decode(clientPublicKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("invalid public key PEM")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   clientID,
			Organization: []string{"Aether"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, m.caCert, pub, m.caKey)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}

// ==================== JWT ====================

func (m *Manager) initJWT() error {
	if data, err := os.ReadFile(m.jwtFile); err == nil {
		m.jwtSecret = data
		return nil
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	if err := os.WriteFile(m.jwtFile, secret, 0600); err != nil {
		return fmt.Errorf("save JWT secret: %w", err)
	}
	m.jwtSecret = secret
	return nil
}

// GenerateToken 生成 JWT Token
func (m *Manager) GenerateToken(apiKey string) (string, error) {
	claims := JWTClaims{
		APIKey: apiKey,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(m.jwtSecret)
}

// ValidateToken 验证 JWT Token
func (m *Manager) ValidateToken(tokenString string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		return m.jwtSecret, nil
	})
	if err != nil {
		return nil, err
	}
	if claims, ok := token.Claims.(*JWTClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, fmt.Errorf("invalid token")
}

// ==================== Auth ====================

// ValidateAPIKey 验证 API Key（常量时间比较，防止时序攻击）
func (m *Manager) ValidateAPIKey(apiKey string) bool {
	return subtle.ConstantTimeCompare([]byte(apiKey), []byte(m.config.APIKey)) == 1
}

// ValidateClientToken 验证客户端 Token（常量时间比较，防止时序攻击）
func (m *Manager) ValidateClientToken(token string) bool {
	return subtle.ConstantTimeCompare([]byte(token), []byte(m.config.ClientToken)) == 1
}

// ==================== Registry ====================

func (m *Manager) loadRegistry() {
	path := filepath.Join(m.config.DataDir, "registry.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if err := json.Unmarshal(data, &m.clients); err != nil {
		fmt.Fprintf(os.Stderr, "warning: 解析 registry.json 失败: %v\n", err)
	}
}

func (m *Manager) saveRegistry() error {
	path := filepath.Join(m.config.DataDir, "registry.json")
	data, err := json.MarshalIndent(m.clients, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal clients: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func (m *Manager) initDefaultNamespaces() {
	defaults := []struct {
		name string
		desc string
		typ  string
	}{
		{"temporary", "临时客户端", "system"},
		{"permanent", "永久客户端", "system"},
		{"mediated", "中继客户端", "system"},
	}

	for _, d := range defaults {
		if _, exists := m.namespaces[d.name]; !exists {
			m.namespaces[d.name] = &NamespaceInfo{
				Name:        d.name,
				Description: d.desc,
				Type:        d.typ,
				Clients:     []string{},
				CreatedAt:   time.Now().Unix(),
			}
		}
	}
}

// ==================== Voucher ====================

func (m *Manager) loadVouchers() {
	path := filepath.Join(m.config.DataDir, "vouchers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if err := json.Unmarshal(data, &m.vouchers); err != nil {
		fmt.Fprintf(os.Stderr, "warning: 解析 vouchers.json 失败: %v\n", err)
	}
}

func (m *Manager) saveVouchers() error {
	path := filepath.Join(m.config.DataDir, "vouchers.json")
	data, err := json.MarshalIndent(m.vouchers, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal vouchers: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

// CreateVoucher 创建凭证
func (m *Manager) CreateVoucher(maxUses int, expiresAt int64, namespace, role string) (*Voucher, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	code := "vch_" + generateRandomHex(12)
	v := &Voucher{
		Code:      code,
		MaxUses:   maxUses,
		UsedCount: 0,
		ExpiresAt: expiresAt,
		Namespace: namespace,
		Role:      role,
		CreatedAt: time.Now().Unix(),
	}
	m.vouchers[code] = v
	if err := m.saveVouchers(); err != nil {
		delete(m.vouchers, code)
		return nil, err
	}
	return v, nil
}

// UseVoucher 使用凭证（校验 + 消耗次数）
func (m *Manager) UseVoucher(code string) (*Voucher, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, exists := m.vouchers[code]
	if !exists {
		return nil, fmt.Errorf("凭证不存在")
	}
	if v.ExpiresAt > 0 && time.Now().Unix() > v.ExpiresAt {
		return nil, fmt.Errorf("凭证已过期")
	}
	if v.MaxUses > 0 && v.UsedCount >= v.MaxUses {
		return nil, fmt.Errorf("凭证已用完")
	}

	v.UsedCount++
	if err := m.saveVouchers(); err != nil {
		v.UsedCount--
		return nil, err
	}
	return v, nil
}

// ListVouchers 列出所有凭证
func (m *Manager) ListVouchers() []*Voucher {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Voucher, 0, len(m.vouchers))
	for _, v := range m.vouchers {
		result = append(result, v)
	}
	return result
}

// DeleteVoucher 删除凭证
func (m *Manager) DeleteVoucher(code string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.vouchers[code]; !exists {
		return false
	}
	delete(m.vouchers, code)
	m.saveVouchers()
	return true
}

// generateRandomHex 生成随机 hex 字符串
func generateRandomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// AddApplication 添加注册申请
func (m *Manager) AddApplication(clientID, publicKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.clients[clientID]; exists {
		return fmt.Errorf("client already exists")
	}

	m.clients[clientID] = &ClientRecord{
		ClientID:  clientID,
		PublicKey: publicKey,
		Status:    "pending",
		Namespace: "permanent",
		Role:      "node",
		CreatedAt: time.Now().Unix(),
	}
	if err := m.saveRegistry(); err != nil {
		delete(m.clients, clientID)
		return err
	}
	return nil
}

// UpdatePublicKey 更新客户端公钥（重新签发时使用）
func (m *Manager) UpdatePublicKey(clientID, publicKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, exists := m.clients[clientID]
	if !exists {
		return fmt.Errorf("client not found")
	}
	client.PublicKey = publicKey
	client.Certificate = ""
	client.Status = "pending"
	return m.saveRegistry()
}

// ResetClientStatus 重置客户端状态为 pending（重新签发证书时使用）
func (m *Manager) ResetClientStatus(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if client, exists := m.clients[clientID]; exists {
		client.Status = "pending"
		client.Certificate = ""
		m.saveRegistry()
	}
}

// Approve 批准注册
func (m *Manager) Approve(clientID string, namespace, role string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, exists := m.clients[clientID]
	if !exists {
		return nil, fmt.Errorf("client not found")
	}

	if client.Status != "pending" {
		return nil, fmt.Errorf("client already %s", client.Status)
	}

	// 签名证书
	certPEM, err := m.signClientCertificate([]byte(client.PublicKey), clientID, 365)
	if err != nil {
		return nil, err
	}

	client.Status = "approved"
	client.Certificate = string(certPEM)
	client.Namespace = namespace
	client.Role = role
	client.ApprovedAt = time.Now().Unix()

	// 添加到命名空间
	if ns, ok := m.namespaces[namespace]; ok {
		ns.Clients = append(ns.Clients, clientID)
	}

	if err := m.saveRegistry(); err != nil {
		return nil, err
	}
	return certPEM, nil
}

// Delete 删除客户端
func (m *Manager) Delete(clientID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, exists := m.clients[clientID]
	if !exists {
		return false
	}

	// 从命名空间移除
	if ns, ok := m.namespaces[client.Namespace]; ok {
		for i, id := range ns.Clients {
			if id == clientID {
				ns.Clients = append(ns.Clients[:i], ns.Clients[i+1:]...)
				break
			}
		}
	}

	delete(m.clients, clientID)
	if err := m.saveRegistry(); err != nil {
		// Delete 已完成内存删除，持久化失败仅记录日志
		fmt.Fprintf(os.Stderr, "warning: save registry after delete: %v\n", err)
	}
	return true
}

// GetByClientID 获取客户端
func (m *Manager) GetByClientID(clientID string) *ClientRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.clients[clientID]
}

// GetPending 获取待审批客户端
func (m *Manager) GetPending() []*ClientRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*ClientRecord
	for _, c := range m.clients {
		if c.Status == "pending" {
			result = append(result, c)
		}
	}
	return result
}

// GetApproved 获取已批准客户端
func (m *Manager) GetApproved() []*ClientRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*ClientRecord
	for _, c := range m.clients {
		if c.Status == "approved" {
			result = append(result, c)
		}
	}
	return result
}

// ==================== Namespace ====================

// GetNamespace 获取命名空间
func (m *Manager) GetNamespace(name string) *NamespaceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.namespaces[name]
}

// ListNamespaces 列出所有命名空间
func (m *Manager) ListNamespaces() []*NamespaceInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*NamespaceInfo
	for _, ns := range m.namespaces {
		result = append(result, ns)
	}
	return result
}

// GetClientsByNamespace 获取命名空间下的客户端
func (m *Manager) GetClientsByNamespace(namespace string) []*ClientRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ns, ok := m.namespaces[namespace]
	if !ok {
		return nil
	}

	var result []*ClientRecord
	for _, clientID := range ns.Clients {
		if client, exists := m.clients[clientID]; exists {
			result = append(result, client)
		}
	}
	return result
}

// SetClientNamespace 设置客户端命名空间
func (m *Manager) SetClientNamespace(clientID, namespace, role string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, exists := m.clients[clientID]
	if !exists {
		return fmt.Errorf("client not found")
	}

	// 检查新命名空间是否存在
	if _, ok := m.namespaces[namespace]; !ok {
		return fmt.Errorf("namespace %s does not exist", namespace)
	}

	// 从旧命名空间移除
	if oldNS, ok := m.namespaces[client.Namespace]; ok {
		for i, id := range oldNS.Clients {
			if id == clientID {
				oldNS.Clients = append(oldNS.Clients[:i], oldNS.Clients[i+1:]...)
				break
			}
		}
	}

	// 添加到新命名空间
	if newNS, ok := m.namespaces[namespace]; ok {
		newNS.Clients = append(newNS.Clients, clientID)
	}

	client.Namespace = namespace
	client.Role = role
	if err := m.saveRegistry(); err != nil {
		return err
	}
	return nil
}

// ==================== TLS ====================

// TLSConfig 创建 TLS 配置
func (m *Manager) TLSConfig() *tls.Config {
	m.caMu.RLock()
	defer m.caMu.RUnlock()

	pool := x509.NewCertPool()
	pool.AddCert(m.caCert)

	return &tls.Config{
		ClientCAs:  pool,
		ClientAuth: tls.VerifyClientCertIfGiven,
	}
}

// ==================== Helpers ====================

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (m *Manager) signClientCertificate(clientPublicKeyPEM []byte, clientID string, validityDays int) ([]byte, error) {
	m.caMu.Lock()
	defer m.caMu.Unlock()

	block, _ := pem.Decode(clientPublicKeyPEM)
	if block == nil {
		return nil, fmt.Errorf("invalid public key PEM")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   clientID,
			Organization: []string{"Aether"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, m.caCert, pub, m.caKey)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}
