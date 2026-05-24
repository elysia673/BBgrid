package workers

import (
	alog "BBgrid/common/log"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// 错误定义
var (
	ErrInvalidPublicKey = errors.New("invalid public key format")
)

// ClientRecord 客户端注册记录
type ClientRecord struct {
	ClientID    string `json:"client_id"`
	PublicKey   string `json:"public_key"`
	Certificate string `json:"certificate"`
	Status      string `json:"status"` // pending, approved, revoked, temp
	Namespace   string `json:"namespace"`
	Role        string `json:"role"`
	CreatedAt   int64  `json:"created_at"`
	ApprovedAt  int64  `json:"approved_at"`
}

// NamespaceInfo 命名空间信息
type NamespaceInfo struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Type        string   `json:"type"`
	Clients     []string `json:"clients"`
	CreatedAt   int64    `json:"created_at"`
}

// JWTClaims JWT 声明
type JWTClaims struct {
	APIKey string `json:"api_key"`
	jwt.RegisteredClaims
}

// AuthConfig 认证配置
type AuthConfig struct {
	DataDir     string // 数据目录
	APIKey      string // 管理 API Key
	ClientToken string // 客户端注册令牌
}

// AuthWorker 身份验证 Worker
//
// 统一管理：
// - CA 证书管理
// - JWT 签发/验证
// - API Key 校验
// - 客户端注册审核
// - 命名空间管理
type AuthWorker struct {
	config AuthConfig

	// CA 证书
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caMu   sync.RWMutex

	// JWT
	jwtSecret     []byte
	jwtSecretFile string

	// 客户端注册表
	clients    map[string]*ClientRecord
	namespaces map[string]*NamespaceInfo
	file       string
	nsFile     string
	mu         sync.RWMutex

	// 生命周期
	stopCh chan struct{}
}

// NewAuthWorker 创建认证 Worker
func NewAuthWorker(config AuthConfig) *AuthWorker {
	return &AuthWorker{
		config:        config,
		clients:       make(map[string]*ClientRecord),
		namespaces:    make(map[string]*NamespaceInfo),
		file:          filepath.Join(config.DataDir, "registry.json"),
		nsFile:        filepath.Join(config.DataDir, "registry.json.ns"),
		jwtSecretFile: filepath.Join(config.DataDir, "jwt_secret"),
		stopCh:        make(chan struct{}),
	}
}

// Name 返回 Worker 名称
func (w *AuthWorker) Name() string {
	return "auth"
}

// Init 初始化认证 Worker（在 Run 之前调用）
func (w *AuthWorker) Init() error {
	// 初始化 CA
	if err := w.initCA(); err != nil {
		return fmt.Errorf("init CA: %w", err)
	}

	// 初始化 JWT
	if err := w.initJWT(); err != nil {
		return fmt.Errorf("init JWT: %w", err)
	}

	// 加载注册表
	w.loadRegistry()
	w.initDefaultNamespaces()
	w.cleanupTempClients()

	return nil
}

// Run 启动认证 Worker
func (w *AuthWorker) Run() error {
	alog.Info(alog.CatAuth, "AuthWorker 启动")

	// 等待停止信号
	<-w.stopCh
	alog.Info(alog.CatAuth, "AuthWorker 停止")
	return nil
}

// Stop 停止认证 Worker
func (w *AuthWorker) Stop() {
	close(w.stopCh)
}

// ==================== CA 证书管理 ====================

// initCA 初始化 CA 证书
func (w *AuthWorker) initCA() error {
	caCertPath := filepath.Join(w.config.DataDir, "ca.crt")
	caKeyPath := filepath.Join(w.config.DataDir, "ca.key")

	if fileExists(caCertPath) && fileExists(caKeyPath) {
		return w.loadCA(caCertPath, caKeyPath)
	}
	return w.generateCA(caCertPath, caKeyPath)
}

// loadCA 从文件加载 CA 证书和私钥
func (w *AuthWorker) loadCA(caCertPath, caKeyPath string) error {
	certData, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}

	keyData, err := os.ReadFile(caKeyPath)
	if err != nil {
		return err
	}

	certBlock, _ := pem.Decode(certData)
	if certBlock == nil {
		return fmt.Errorf("invalid CA certificate PEM: %s", caCertPath)
	}
	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return err
	}

	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		return fmt.Errorf("invalid CA key PEM: %s", caKeyPath)
	}
	caKey, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return err
	}

	w.caMu.Lock()
	w.caCert = caCert
	w.caKey = caKey
	w.caMu.Unlock()

	alog.Info(alog.CatAuth, "CA 证书已从文件加载")
	return nil
}

// generateCA 生成新的 CA 证书和私钥
func (w *AuthWorker) generateCA(caCertPath, caKeyPath string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "BBgrid CA",
			Organization: []string{"BBgrid"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &key.PublicKey, key)
	if err != nil {
		return err
	}

	caCert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		return err
	}

	// 保存 CA 证书
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	os.MkdirAll(filepath.Dir(caCertPath), 0755)
	os.WriteFile(caCertPath, certPEM, 0644)

	// 保存 CA 私钥
	keyBytes, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	})
	os.WriteFile(caKeyPath, keyPEM, 0600)

	w.caMu.Lock()
	w.caCert = caCert
	w.caKey = key
	w.caMu.Unlock()

	alog.Info(alog.CatAuth, "CA 证书已生成")
	return nil
}

// GetCA 获取 CA 证书（用于 mTLS 配置）
func (w *AuthWorker) GetCA() *x509.Certificate {
	w.caMu.RLock()
	defer w.caMu.RUnlock()
	return w.caCert
}

// SignClientCertificate 签发客户端证书
func (w *AuthWorker) SignClientCertificate(clientPublicKeyPEM []byte, clientID string, validityDays int) ([]byte, error) {
	w.caMu.RLock()
	defer w.caMu.RUnlock()

	if w.caCert == nil || w.caKey == nil {
		return nil, fmt.Errorf("CA not initialized")
	}

	block, _ := pem.Decode(clientPublicKeyPEM)
	if block == nil {
		return nil, ErrInvalidPublicKey
	}

	clientPubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:   clientID,
			Organization: []string{"BBgrid Client"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, w.caCert, clientPubKey, w.caKey)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	return certPEM, nil
}

// ==================== JWT 管理 ====================

// initJWT 初始化 JWT 密钥
func (w *AuthWorker) initJWT() error {
	data, err := os.ReadFile(w.jwtSecretFile)
	if err == nil && len(data) >= 32 {
		w.jwtSecret = data[:32]
		alog.Info(alog.CatAuth, "JWT 密钥已从文件加载")
		return nil
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	w.jwtSecret = b

	os.MkdirAll(filepath.Dir(w.jwtSecretFile), 0755)
	if err := os.WriteFile(w.jwtSecretFile, b, 0600); err != nil {
		alog.Error(alog.CatAuth, "保存 JWT 密钥失败", "error", err)
	}
	alog.Info(alog.CatAuth, "JWT 密钥已随机生成并保存")
	return nil
}

// GenerateToken 生成 JWT Token
func (w *AuthWorker) GenerateToken(apiKey string) (string, error) {
	claims := JWTClaims{
		APIKey: apiKey,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(365 * 24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(w.jwtSecret)
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}

	// 标记为已注册
	w.markKeyRegistered(apiKey)

	return tokenStr, nil
}

// ValidateToken 验证 JWT Token
func (w *AuthWorker) ValidateToken(tokenString string) (*JWTClaims, error) {
	claims := &JWTClaims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return w.jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	// 检查是否被撤销
	if !w.isKeyRegistered(claims.APIKey) {
		return nil, fmt.Errorf("token revoked")
	}

	return claims, nil
}

// isKeyRegistered 检查 API Key 是否已注册
func (w *AuthWorker) isKeyRegistered(apiKey string) bool {
	path := w.getKeyFilePath(apiKey)
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// markKeyRegistered 标记 API Key 为已注册
func (w *AuthWorker) markKeyRegistered(apiKey string) {
	dir := filepath.Join(w.config.DataDir, "registered_keys")
	os.MkdirAll(dir, 0755)
	path := w.getKeyFilePath(apiKey)
	os.WriteFile(path, []byte("registered"), 0644)
}

// getKeyFilePath 获取 API Key 文件路径
func (w *AuthWorker) getKeyFilePath(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	filename := hex.EncodeToString(hash[:])
	return filepath.Join(w.config.DataDir, "registered_keys", filename)
}

// ValidateAPIKey 验证 API Key
func (w *AuthWorker) ValidateAPIKey(apiKey string) bool {
	return subtle.ConstantTimeCompare([]byte(apiKey), []byte(w.config.APIKey)) == 1
}

// ValidateClientToken 验证客户端令牌
func (w *AuthWorker) ValidateClientToken(token string) bool {
	return subtle.ConstantTimeCompare([]byte(token), []byte(w.config.ClientToken)) == 1
}

// ==================== 客户端注册表 ====================

// loadRegistry 从文件加载注册表
func (w *AuthWorker) loadRegistry() {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(w.file)
	if err != nil {
		return
	}
	json.Unmarshal(data, &w.clients)

	nsData, err := os.ReadFile(w.nsFile)
	if err != nil {
		return
	}
	json.Unmarshal(nsData, &w.namespaces)
}

// saveRegistry 保存注册表到文件
func (w *AuthWorker) saveRegistry() {
	data, _ := json.MarshalIndent(w.clients, "", "  ")
	if err := os.WriteFile(w.file, data, 0644); err != nil {
		alog.Error(alog.CatAuth, "保存注册表失败", "error", err)
	}

	nsData, _ := json.MarshalIndent(w.namespaces, "", "  ")
	if err := os.WriteFile(w.nsFile, nsData, 0644); err != nil {
		alog.Error(alog.CatAuth, "保存命名空间失败", "error", err)
	}
}

// initDefaultNamespaces 初始化默认命名空间
func (w *AuthWorker) initDefaultNamespaces() {
	w.mu.Lock()
	defer w.mu.Unlock()

	defaults := map[string]*NamespaceInfo{
		"temporary": {
			Name:        "temporary",
			Description: "临时命名空间，临时节点默认分配到此",
			Type:        "temporary",
			Clients:     []string{},
			CreatedAt:   time.Now().Unix(),
		},
		"permanent": {
			Name:        "permanent",
			Description: "常驻命名空间，常驻节点默认分配到此",
			Type:        "permanent",
			Clients:     []string{},
			CreatedAt:   time.Now().Unix(),
		},
		"mediated": {
			Name:        "mediated",
			Description: "受限桥接区域，跨角色通信使用",
			Type:        "mediated",
			Clients:     []string{},
			CreatedAt:   time.Now().Unix(),
		},
	}

	for name, ns := range defaults {
		if _, exists := w.namespaces[name]; !exists {
			w.namespaces[name] = ns
		}
	}
}

// cleanupTempClients 清理临时客户端
func (w *AuthWorker) cleanupTempClients() {
	w.mu.Lock()
	defer w.mu.Unlock()

	count := 0
	for clientID, record := range w.clients {
		if record.Status == "temp" {
			w.removeFromNamespace(clientID, record.Namespace)
			delete(w.clients, clientID)
			count++
		}
	}

	if count > 0 {
		w.saveRegistry()
		alog.Info(alog.CatAuth, "清理临时客户端完成", "count", count)
	}
}

// removeFromNamespace 从命名空间移除客户端
func (w *AuthWorker) removeFromNamespace(clientID, namespace string) {
	if namespace == "" {
		return
	}
	ns, ok := w.namespaces[namespace]
	if !ok {
		return
	}
	for i, id := range ns.Clients {
		if id == clientID {
			ns.Clients = append(ns.Clients[:i], ns.Clients[i+1:]...)
			break
		}
	}
}

// AddApplication 添加注册申请
func (w *AuthWorker) AddApplication(clientID, publicKey string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if existing, exists := w.clients[clientID]; exists {
		if existing.Status == "approved" {
			return fmt.Errorf("client %s already approved", clientID)
		}
		existing.PublicKey = publicKey
		existing.CreatedAt = time.Now().Unix()
		w.saveRegistry()
		return nil
	}

	w.clients[clientID] = &ClientRecord{
		ClientID:  clientID,
		PublicKey: publicKey,
		Status:    "pending",
		CreatedAt: time.Now().Unix(),
	}
	w.saveRegistry()
	return nil
}

// Approve 审核通过并签发证书
func (w *AuthWorker) Approve(clientID string, namespace, role string) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	record, exists := w.clients[clientID]
	if !exists {
		return nil, fmt.Errorf("client %s not found", clientID)
	}

	if record.Status != "pending" {
		return nil, fmt.Errorf("client %s is not pending", clientID)
	}

	certPEM, err := w.signClientCertificate([]byte(record.PublicKey), clientID, 365)
	if err != nil {
		return nil, err
	}

	record.Certificate = string(certPEM)
	record.Status = "approved"
	record.ApprovedAt = time.Now().Unix()

	if namespace == "" {
		namespace = "permanent"
	}
	if role == "" {
		role = "permanent"
	}
	record.Namespace = namespace
	record.Role = role

	if ns, ok := w.namespaces[namespace]; ok {
		found := false
		for _, id := range ns.Clients {
			if id == clientID {
				found = true
				break
			}
		}
		if !found {
			ns.Clients = append(ns.Clients, clientID)
		}
	}

	w.saveRegistry()
	return certPEM, nil
}

// signClientCertificate 内部方法，不加锁
func (w *AuthWorker) signClientCertificate(clientPublicKeyPEM []byte, clientID string, validityDays int) ([]byte, error) {
	if w.caCert == nil || w.caKey == nil {
		return nil, fmt.Errorf("CA not initialized")
	}

	block, _ := pem.Decode(clientPublicKeyPEM)
	if block == nil {
		return nil, ErrInvalidPublicKey
	}

	clientPubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:   clientID,
			Organization: []string{"BBgrid Client"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Duration(validityDays) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &template, w.caCert, clientPubKey, w.caKey)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}), nil
}

// Delete 删除客户端
func (w *AuthWorker) Delete(clientID string) bool {
	w.mu.Lock()
	if record, exists := w.clients[clientID]; exists {
		w.removeFromNamespace(clientID, record.Namespace)
		delete(w.clients, clientID)
		w.saveRegistry()
		w.mu.Unlock()
		return true
	}
	w.mu.Unlock()
	return false
}

// AddTempClient 添加临时客户端
func (w *AuthWorker) AddTempClient(clientID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if existing, exists := w.clients[clientID]; exists {
		if existing.Status == "approved" {
			return fmt.Errorf("client %s already exists as permanent node", clientID)
		}
	}

	w.clients[clientID] = &ClientRecord{
		ClientID:  clientID,
		Status:    "temp",
		Namespace: "temporary",
		Role:      "temporary",
		CreatedAt: time.Now().Unix(),
	}

	if ns, ok := w.namespaces["temporary"]; ok {
		found := false
		for _, id := range ns.Clients {
			if id == clientID {
				found = true
				break
			}
		}
		if !found {
			ns.Clients = append(ns.Clients, clientID)
		}
	}

	w.saveRegistry()
	return nil
}

// RemoveTempClient 移除临时客户端
func (w *AuthWorker) RemoveTempClient(clientID string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	record, exists := w.clients[clientID]
	if !exists || record.Status != "temp" {
		return
	}

	w.removeFromNamespace(clientID, record.Namespace)
	delete(w.clients, clientID)
	w.saveRegistry()
	alog.Info(alog.CatAuth, "临时客户端已移除", "clientID", clientID)
}

// GetByClientID 获取客户端记录
func (w *AuthWorker) GetByClientID(clientID string) *ClientRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.clients[clientID]
}

// GetPending 获取待审核列表
func (w *AuthWorker) GetPending() []*ClientRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range w.clients {
		if record.Status == "pending" {
			records = append(records, record)
		}
	}
	return records
}

// GetApproved 获取已通过列表
func (w *AuthWorker) GetApproved() []*ClientRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range w.clients {
		if record.Status == "approved" {
			records = append(records, record)
		}
	}
	return records
}

// GetNamespace 获取命名空间
func (w *AuthWorker) GetNamespace(name string) *NamespaceInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.namespaces[name]
}

// ListNamespaces 获取所有命名空间
func (w *AuthWorker) ListNamespaces() []*NamespaceInfo {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var namespaces []*NamespaceInfo
	for _, ns := range w.namespaces {
		namespaces = append(namespaces, ns)
	}
	return namespaces
}

// GetClientsByNamespace 获取命名空间下的客户端
func (w *AuthWorker) GetClientsByNamespace(namespace string) []*ClientRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range w.clients {
		if record.Namespace == namespace {
			records = append(records, record)
		}
	}
	return records
}

// SetClientNamespace 设置客户端命名空间
func (w *AuthWorker) SetClientNamespace(clientID, namespace, role string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record, exists := w.clients[clientID]
	if !exists {
		return fmt.Errorf("client %s not found", clientID)
	}

	w.removeFromNamespace(clientID, record.Namespace)

	record.Namespace = namespace
	record.Role = role

	if ns, ok := w.namespaces[namespace]; ok {
		found := false
		for _, id := range ns.Clients {
			if id == clientID {
				found = true
				break
			}
		}
		if !found {
			ns.Clients = append(ns.Clients, clientID)
		}
	}

	w.saveRegistry()
	return nil
}

// ==================== 辅助函数 ====================

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
