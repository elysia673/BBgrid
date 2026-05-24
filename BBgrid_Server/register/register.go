package register

import (
	alog "BBgrid/common/log"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"
)

var (
	caCert *x509.Certificate // CA 证书
	caKey  *ecdsa.PrivateKey // CA 私钥

	ErrInvalidPublicKey = errors.New("invalid public key format")
)

// ClientRecord 客户端注册记录
type ClientRecord struct {
	ClientID    string `json:"client_id"`   // 客户端 ID
	PublicKey   string `json:"public_key"`  // 公钥（PEM 格式）
	Certificate string `json:"certificate"` // 签发的证书（PEM 格式，已通过时有值）
	Status      string `json:"status"`      // 状态: pending(待审核), approved(已通过), revoked(已吊销)
	CreatedAt   int64  `json:"created_at"`  // 创建时间戳
	ApprovedAt  int64  `json:"approved_at"` // 审核通过时间戳
}

// Registry 客户端注册表，管理所有已注册的客户端
type Registry struct {
	mu             sync.RWMutex
	clients        map[string]*ClientRecord
	file           string                // 持久化文件路径
	onClientDelete func(clientID string) // 客户端删除回调
}

// NewRegistry 创建注册表实例，自动加载已有数据
func NewRegistry(file string) *Registry {
	r := &Registry{
		clients: make(map[string]*ClientRecord),
		file:    file,
	}
	r.load()
	return r
}

// SetOnClientDelete 设置客户端删除回调
func (r *Registry) SetOnClientDelete(callback func(clientID string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onClientDelete = callback
}

// AddApplication 添加注册申请（待审核）
func (r *Registry) AddApplication(clientID, publicKey string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果已存在
	if existing, exists := r.clients[clientID]; exists {
		if existing.Status == "approved" {
			return fmt.Errorf("client %s already approved", clientID)
		}
		// pending 状态直接覆盖
		existing.PublicKey = publicKey
		existing.CreatedAt = time.Now().Unix()
		r.save()
		return nil
	}

	r.clients[clientID] = &ClientRecord{
		ClientID:  clientID,
		PublicKey: publicKey,
		Status:    "pending",
		CreatedAt: time.Now().Unix(),
	}
	r.save()
	return nil
}

// Approve 审核通过并签发证书
func (r *Registry) Approve(clientID string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, exists := r.clients[clientID]
	if !exists {
		return nil, fmt.Errorf("client %s not found", clientID)
	}

	if record.Status != "pending" {
		return nil, fmt.Errorf("client %s is not pending", clientID)
	}

	// 签发证书
	certPEM, err := SignClientCertificate([]byte(record.PublicKey), clientID, 365)
	if err != nil {
		return nil, err
	}

	// 更新状态
	record.Certificate = string(certPEM)
	record.Status = "approved"
	record.ApprovedAt = time.Now().Unix()
	r.save()

	return certPEM, nil
}

// Delete 吊销/删除客户端
func (r *Registry) Delete(clientID string) bool {
	r.mu.Lock()
	if _, exists := r.clients[clientID]; exists {
		delete(r.clients, clientID)
		alog.Info(alog.CatAuth, "已从内存删除客户端 clientID=%s currentCount=%d", clientID, len(r.clients))
		r.save()
		callback := r.onClientDelete
		r.mu.Unlock()

		// 调用回调断开客户端连接
		if callback != nil {
			callback(clientID)
		}
		return true
	}
	r.mu.Unlock()
	return false
}

// GetPending 获取待审核列表
func (r *Registry) GetPending() []*ClientRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range r.clients {
		if record.Status == "pending" {
			records = append(records, record)
		}
	}
	return records
}

// GetApproved 获取已通过列表
func (r *Registry) GetApproved() []*ClientRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range r.clients {
		if record.Status == "approved" {
			records = append(records, record)
		}
	}
	return records
}

// IsRevoked 检查客户端是否已吊销（保留兼容性）
func (r *Registry) IsRevoked(clientID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if record, exists := r.clients[clientID]; exists {
		return record.Status == "revoked"
	}
	return false
}

// List 获取所有记录（保留兼容性）
func (r *Registry) List() []*ClientRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var records []*ClientRecord
	for _, record := range r.clients {
		records = append(records, record)
	}
	return records
}

// GetByClientID 根据客户端 ID 获取记录
func (r *Registry) GetByClientID(clientID string) *ClientRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if record, exists := r.clients[clientID]; exists {
		return record
	}
	return nil
}

// load 从文件加载注册表数据
func (r *Registry) load() {
	data, err := os.ReadFile(r.file)
	if err != nil {
		return
	}
	json.Unmarshal(data, &r.clients)
}

// save 保存注册表数据到文件
func (r *Registry) save() {
	data, _ := json.MarshalIndent(r.clients, "", "  ")
	alog.Info(alog.CatAuth, "保存注册表 path=%s clientCount=%d", r.file, len(r.clients))
	if err := os.WriteFile(r.file, data, 0644); err != nil {
		alog.Error(alog.CatAuth, "保存注册表失败 error=%s", err)
	}
}

// fileExists 检查文件是否存在
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// loadCA 从文件加载 CA 证书和私钥
func loadCA(caCertPath, caKeyPath string) error {
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
	caCert, err = x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return err
	}

	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		return fmt.Errorf("invalid CA key PEM: %s", caKeyPath)
	}
	caKey, err = x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return err
	}

	return nil
}

// generateCA 生成新的 CA 证书和私钥并保存到文件
func generateCA(caCertPath, caKeyPath string) error {
	// 生成 ECC P-256 私钥
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caKey = key

	// 创建 CA 证书模板
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

	// 自签名 CA 证书
	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}

	caCert, err = x509.ParseCertificate(caBytes)
	if err != nil {
		return err
	}

	// 保存 CA 证书（PEM 格式）
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caBytes,
	})
	os.WriteFile(caCertPath, certPEM, 0644)

	// 保存 CA 私钥（PEM 格式）
	keyBytes, _ := x509.MarshalECPrivateKey(caKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	})
	os.WriteFile(caKeyPath, keyPEM, 0600)

	return nil
}

// InitCA 初始化 CA，如果证书不存在则自动生成
func InitCA(caCertPath, caKeyPath string) error {
	if fileExists(caCertPath) && fileExists(caKeyPath) {
		return loadCA(caCertPath, caKeyPath)
	}
	return generateCA(caCertPath, caKeyPath)
}

// SignClientCertificate 用 CA 私钥签发客户端证书
func SignClientCertificate(clientPublicKeyPEM []byte, clientID string, validityDays int) ([]byte, error) {
	// 解析客户端公钥
	block, _ := pem.Decode(clientPublicKeyPEM)
	if block == nil {
		return nil, ErrInvalidPublicKey
	}

	clientPubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	// 创建客户端证书模板
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

	// 用 CA 私钥签名
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, clientPubKey, caKey)
	if err != nil {
		return nil, err
	}

	// 返回 PEM 格式证书
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	})

	return certPEM, nil
}

// GetCA 获取 CA 证书和私钥（用于 mTLS 配置）
func GetCA() (*x509.Certificate, *ecdsa.PrivateKey) {
	return caCert, caKey
}
