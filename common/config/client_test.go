package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadClient_GenerateKeyPair(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/client.json"
	dataDir := tmpDir + "/data"

	configContent := `{
		"server_url": "wss://example.com:9909/ws",
		"client_token": "test-token-123",
		"client_id": "test-client",
		"private_key_path": "` + dataDir + `/client.key",
		"public_key_path": "` + dataDir + `/client.pub",
		"certificate_path": "` + dataDir + `/client.crt"
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("写入配置文件失败: %v", err)
	}

	cfg, err := LoadClient(configPath)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	if cfg.ServerURL != "wss://example.com:9909/ws" {
		t.Errorf("ServerURL 错误: %s", cfg.ServerURL)
	}

	if cfg.ClientToken != "test-token-123" {
		t.Errorf("ClientToken 错误: %s", cfg.ClientToken)
	}

	if !fileExists(cfg.PrivateKeyPath) {
		t.Error("私钥文件未生成")
	}

	if !fileExists(cfg.PublicKeyPath) {
		t.Error("公钥文件未生成")
	}

	privateData, err := os.ReadFile(cfg.PrivateKeyPath)
	if err != nil {
		t.Fatalf("读取私钥失败: %v", err)
	}

	if len(privateData) == 0 {
		t.Error("私钥文件为空")
	}
}

func TestLoadClient_ExistingKeys(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/client.json"
	dataDir := tmpDir + "/data"

	os.MkdirAll(dataDir, 0755)

	os.WriteFile(dataDir+"/client.key", []byte("existing-key"), 0600)
	os.WriteFile(dataDir+"/client.pub", []byte("existing-pub"), 0644)

	configContent := `{
		"server_url": "wss://example.com:9909/ws",
		"client_token": "test-token",
		"private_key_path": "` + dataDir + `/client.key",
		"public_key_path": "` + dataDir + `/client.pub"
	}`

	os.WriteFile(configPath, []byte(configContent), 0644)

	cfg, err := LoadClient(configPath)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	keyData, _ := os.ReadFile(cfg.PrivateKeyPath)
	if string(keyData) != "existing-key" {
		t.Error("已存在的密钥被覆盖")
	}
}

func TestLoadClient_MissingRequiredFields(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/client.json"

	configContent := `{
		"client_id": "test-client"
	}`

	os.WriteFile(configPath, []byte(configContent), 0644)

	_, err := LoadClient(configPath)
	if err == nil {
		t.Fatal("期望错误，得到 nil")
	}
}

func TestLoadClient_DefaultPaths(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := tmpDir + "/client.json"

	configContent := `{
		"server_url": "wss://example.com:9909/ws",
		"client_token": "test-token"
	}`

	os.WriteFile(configPath, []byte(configContent), 0644)

	cfg, err := LoadClient(configPath)
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	expectedKeyPath := filepath.Join("data", "client.key")
	if cfg.PrivateKeyPath != expectedKeyPath {
		t.Errorf("PrivateKeyPath 错误: 期望 %s, 得到 %s", expectedKeyPath, cfg.PrivateKeyPath)
	}
}

func TestFileExists(t *testing.T) {
	tmpDir := t.TempDir()
	existingFile := filepath.Join(tmpDir, "exists.txt")
	nonExistingFile := filepath.Join(tmpDir, "not_exists.txt")

	os.WriteFile(existingFile, []byte("test"), 0644)

	if !fileExists(existingFile) {
		t.Error("现有文件应返回 true")
	}

	if fileExists(nonExistingFile) {
		t.Error("不存在文件应返回 false")
	}
}
