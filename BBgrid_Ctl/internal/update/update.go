// Package update 提供更新管理功能。
package update

import (
	"BBgrid/BBgrid_Ctl/internal/process"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// UpdateInfo 更新信息
type UpdateInfo struct {
	Version string `json:"version"`
	URL     string `json:"url"`
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Notes   string `json:"notes"`
}

// Manager 更新管理器
type Manager struct {
	baseDir     string
	binDir      string
	versionsDir string
	updateURL   string
	httpClient  *http.Client
	processMgr  *process.Manager
}

// NewManager 创建更新管理器
func NewManager(updateURL string) *Manager {
	base := getBaseDir()
	return &Manager{
		baseDir:     base,
		binDir:      filepath.Join(base, "bin"),
		versionsDir: filepath.Join(base, "versions"),
		updateURL:   updateURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
		processMgr: process.NewManager(),
	}
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	execPath, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(filepath.Dir(execPath))
}

// CheckForUpdate 检查更新
func (m *Manager) CheckForUpdate(target string, currentVersion string) (*UpdateInfo, error) {
	url := fmt.Sprintf("%s/api/v1/update/check?target=%s&version=%s&os=%s&arch=%s",
		m.updateURL, target, currentVersion, runtime.GOOS, runtime.GOARCH)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("check update: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil // 没有更新
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("check update: status %d", resp.StatusCode)
	}

	var info UpdateInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode update info: %w", err)
	}

	return &info, nil
}

// Update 执行更新
func (m *Manager) Update(target string, info *UpdateInfo) error {
	fmt.Printf("Updating %s to version %s...\n", target, info.Version)

	// 1. 下载新版本
	tmpPath := filepath.Join(m.baseDir, "data", "tmp", fmt.Sprintf("bbgrid-%s-%s", target, info.Version))
	if runtime.GOOS == "windows" {
		tmpPath += ".exe"
	}

	fmt.Println("  Downloading...")
	if err := m.downloadFile(info.URL, tmpPath); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer os.Remove(tmpPath)

	// 2. 校验 SHA256
	if info.SHA256 != "" {
		fmt.Println("  Verifying checksum...")
		if err := m.verifySHA256(tmpPath, info.SHA256); err != nil {
			return fmt.Errorf("verify: %w", err)
		}
	}

	// 3. 停止服务
	fmt.Println("  Stopping service...")
	if err := m.processMgr.Stop(target); err != nil {
		// 忽略停止错误
		fmt.Printf("  Warning: stop failed: %v\n", err)
	}
	time.Sleep(500 * time.Millisecond)

	// 4. 备份当前版本
	fmt.Println("  Backing up current version...")
	if err := m.backup(target); err != nil {
		fmt.Printf("  Warning: backup failed: %v\n", err)
	}

	// 5. 替换二进制
	fmt.Println("  Replacing binary...")
	binPath := m.getBinPath(target)
	if err := os.Rename(tmpPath, binPath); err != nil {
		// 如果 rename 失败，尝试复制
		if err := m.copyFile(tmpPath, binPath); err != nil {
			return fmt.Errorf("replace: %w", err)
		}
	}
	os.Chmod(binPath, 0755)

	// 6. 启动服务
	fmt.Println("  Starting service...")
	if err := m.processMgr.Start(target); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	fmt.Printf("  %s updated to version %s\n", target, info.Version)
	return nil
}

// Rollback 回滚
func (m *Manager) Rollback(target string) error {
	backupDir := filepath.Join(m.versionsDir, target)

	// 查找最新的备份
	entries, err := os.ReadDir(backupDir)
	if err != nil || len(entries) == 0 {
		return fmt.Errorf("no backup found for %s", target)
	}

	// 获取最新的备份
	latestBackup := entries[len(entries)-1]
	backupPath := filepath.Join(backupDir, latestBackup.Name())

	// 停止服务
	m.processMgr.Stop(target)
	time.Sleep(500 * time.Millisecond)

	// 恢复备份
	binPath := m.getBinPath(target)
	if err := m.copyFile(backupPath, binPath); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	os.Chmod(binPath, 0755)

	// 启动服务
	if err := m.processMgr.Start(target); err != nil {
		return fmt.Errorf("start: %w", err)
	}

	fmt.Printf("  %s rolled back to %s\n", target, latestBackup.Name())
	return nil
}

// backup 备份当前版本
func (m *Manager) backup(target string) error {
	binPath := m.getBinPath(target)
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		return nil // 没有当前版本，跳过备份
	}

	backupDir := filepath.Join(m.versionsDir, target)
	os.MkdirAll(backupDir, 0755)

	// 使用时间戳作为备份名
	timestamp := time.Now().Format("20060102-150405")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("bbgrid-%s-%s", target, timestamp))

	return m.copyFile(binPath, backupPath)
}

// downloadFile 下载文件
func (m *Manager) downloadFile(url, destPath string) error {
	// 确保目录存在
	os.MkdirAll(filepath.Dir(destPath), 0755)

	resp, err := m.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: status %d", resp.StatusCode)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

// verifySHA256 校验 SHA256
func (m *Manager) verifySHA256(filePath, expected string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	hash := sha256.Sum256(data)
	actual := hex.EncodeToString(hash[:])

	if actual != expected {
		return fmt.Errorf("SHA256 mismatch: expected %s, got %s", expected, actual)
	}

	return nil
}

// copyFile 复制文件
func (m *Manager) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// getBinPath 获取二进制路径
func (m *Manager) getBinPath(name string) string {
	binName := "bbgrid-" + name
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	return filepath.Join(m.binDir, binName)
}
