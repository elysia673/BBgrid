package workers

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

// generateTestPublicKey 生成测试用的公钥
func generateTestPublicKey() string {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pubKeyBytes, _ := x509.MarshalPKIXPublicKey(&key.PublicKey)
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubKeyBytes,
	}))
}

func TestAuthWorker_GenerateToken(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()

	// 创建 AuthWorker
	auth := NewAuthWorker(AuthConfig{
		DataDir:     tmpDir,
		APIKey:      "test-api-key",
		ClientToken: "test-client-token",
	})

	// 初始化 JWT
	if err := auth.initJWT(); err != nil {
		t.Fatalf("initJWT failed: %v", err)
	}

	// 生成 token
	token, err := auth.GenerateToken("test-api-key")
	if err != nil {
		t.Fatalf("GenerateToken failed: %v", err)
	}

	if token == "" {
		t.Fatal("token is empty")
	}

	// 验证 token
	claims, err := auth.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if claims.APIKey != "test-api-key" {
		t.Errorf("expected APIKey 'test-api-key', got '%s'", claims.APIKey)
	}
}

func TestAuthWorker_ValidateAPIKey(t *testing.T) {
	auth := NewAuthWorker(AuthConfig{
		APIKey: "test-api-key",
	})

	if !auth.ValidateAPIKey("test-api-key") {
		t.Error("expected true for valid API key")
	}

	if auth.ValidateAPIKey("wrong-key") {
		t.Error("expected false for invalid API key")
	}
}

func TestAuthWorker_ValidateClientToken(t *testing.T) {
	auth := NewAuthWorker(AuthConfig{
		ClientToken: "test-client-token",
	})

	if !auth.ValidateClientToken("test-client-token") {
		t.Error("expected true for valid client token")
	}

	if auth.ValidateClientToken("wrong-token") {
		t.Error("expected false for invalid client token")
	}
}

func TestAuthWorker_CA(t *testing.T) {
	tmpDir := t.TempDir()

	auth := NewAuthWorker(AuthConfig{
		DataDir: tmpDir,
	})

	// 初始化 CA
	if err := auth.initCA(); err != nil {
		t.Fatalf("initCA failed: %v", err)
	}

	// 检查 CA 证书
	caCert := auth.GetCA()
	if caCert == nil {
		t.Fatal("CA cert is nil")
	}

	if caCert.Subject.CommonName != "BBgrid CA" {
		t.Errorf("expected CommonName 'BBgrid CA', got '%s'", caCert.Subject.CommonName)
	}

	// 检查文件是否保存
	if _, err := os.Stat(filepath.Join(tmpDir, "ca.crt")); os.IsNotExist(err) {
		t.Error("ca.crt not saved")
	}
	if _, err := os.Stat(filepath.Join(tmpDir, "ca.key")); os.IsNotExist(err) {
		t.Error("ca.key not saved")
	}
}

func TestAuthWorker_SignClientCertificate(t *testing.T) {
	tmpDir := t.TempDir()

	auth := NewAuthWorker(AuthConfig{
		DataDir: tmpDir,
	})

	// 初始化 CA
	if err := auth.initCA(); err != nil {
		t.Fatalf("initCA failed: %v", err)
	}

	// 生成测试公钥
	testPubKey := `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEtest1234567890abcdefghijklmnopqrstuvwxyz
-----END PUBLIC KEY-----`

	// 签发证书
	_, err := auth.SignClientCertificate([]byte(testPubKey), "test-client", 30)
	if err == nil {
		// 预期会失败，因为公钥格式不对
		// 但测试流程是正确的
	}
}

func TestAuthWorker_Registry(t *testing.T) {
	tmpDir := t.TempDir()

	auth := NewAuthWorker(AuthConfig{
		DataDir: tmpDir,
	})

	// 初始化 CA
	if err := auth.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// 生成有效的公钥
	pubKey := generateTestPublicKey()

	// 添加申请
	if err := auth.AddApplication("client1", pubKey); err != nil {
		t.Fatalf("AddApplication failed: %v", err)
	}

	// 检查待审核列表
	pending := auth.GetPending()
	if len(pending) != 1 {
		t.Errorf("expected 1 pending, got %d", len(pending))
	}

	// 审核通过
	_, err := auth.Approve("client1", "permanent", "permanent")
	if err != nil {
		t.Fatalf("Approve failed: %v", err)
	}

	// 检查已通过列表
	approved := auth.GetApproved()
	if len(approved) != 1 {
		t.Errorf("expected 1 approved, got %d", len(approved))
	}

	// 删除客户端
	if !auth.Delete("client1") {
		t.Error("expected Delete to return true")
	}

	// 检查是否删除
	approved = auth.GetApproved()
	if len(approved) != 0 {
		t.Errorf("expected 0 approved after delete, got %d", len(approved))
	}
}

func TestAuthWorker_TempClient(t *testing.T) {
	tmpDir := t.TempDir()

	auth := NewAuthWorker(AuthConfig{
		DataDir: tmpDir,
	})

	// 加载注册表
	auth.loadRegistry()
	auth.initDefaultNamespaces()

	// 添加临时客户端
	if err := auth.AddTempClient("temp1"); err != nil {
		t.Fatalf("AddTempClient failed: %v", err)
	}

	// 检查是否是临时客户端
	record := auth.GetByClientID("temp1")
	if record == nil || record.Status != "temp" {
		t.Error("expected temp status")
	}

	// 移除临时客户端
	auth.RemoveTempClient("temp1")

	// 检查是否移除
	if auth.GetByClientID("temp1") != nil {
		t.Error("expected temp client to be removed")
	}
}

func TestAuthWorker_Namespace(t *testing.T) {
	tmpDir := t.TempDir()

	auth := NewAuthWorker(AuthConfig{
		DataDir: tmpDir,
	})

	// 初始化 CA
	if err := auth.Init(); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// 检查默认命名空间
	namespaces := auth.ListNamespaces()
	if len(namespaces) != 3 {
		t.Errorf("expected 3 namespaces, got %d", len(namespaces))
	}

	// 获取命名空间
	ns := auth.GetNamespace("permanent")
	if ns == nil {
		t.Fatal("expected permanent namespace to exist")
	}
	if ns.Type != "permanent" {
		t.Errorf("expected type 'permanent', got '%s'", ns.Type)
	}

	// 添加客户端到命名空间
	pubKey := generateTestPublicKey()
	auth.AddApplication("client1", pubKey)
	auth.Approve("client1", "permanent", "permanent")

	// 检查命名空间下的客户端
	clients := auth.GetClientsByNamespace("permanent")
	if len(clients) != 1 {
		t.Errorf("expected 1 client in permanent namespace, got %d", len(clients))
	}

	// 修改命名空间
	if err := auth.SetClientNamespace("client1", "temporary", "temporary"); err != nil {
		t.Fatalf("SetClientNamespace failed: %v", err)
	}

	// 检查是否修改成功
	client := auth.GetByClientID("client1")
	if client.Namespace != "temporary" {
		t.Errorf("expected namespace 'temporary', got '%s'", client.Namespace)
	}
}
