// Package process 提供进程管理功能。
package process

import (
	"BBgrid/common/pidfile"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// ServiceStatus 服务状态
type ServiceStatus struct {
	Running bool
	PID     int
}

// Manager 进程管理器
type Manager struct {
	baseDir    string
	binDir     string
	configDir  string
	pidDir     string
	logDir     string
}

// NewManager 创建进程管理器
func NewManager() *Manager {
	base := getBaseDir()
	return &Manager{
		baseDir:   base,
		binDir:    filepath.Join(base, "bin"),
		configDir: filepath.Join(base, "config"),
		pidDir:    pidfile.DefaultDir,
		logDir:    filepath.Join(base, "logs"),
	}
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	// 优先使用环境变量
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	// 默认使用可执行文件所在目录的上一级
	execPath, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(filepath.Dir(execPath))
}

// Start 启动服务
func (m *Manager) Start(name string) error {
	// 检查是否已运行
	if status := m.getStatus(name); status.Running {
		return fmt.Errorf("%s is already running (PID: %d)", name, status.PID)
	}

	// 确保目录存在
	m.ensureDirs()

	// 获取二进制路径
	binPath := m.getBinPath(name)
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		return fmt.Errorf("%s not found: %s", name, binPath)
	}

	// 获取配置路径
	configPath := m.getConfigPath(name)

	// 获取日志路径
	logPath := filepath.Join(m.logDir, name+".log")

	// 构建命令
	args := []string{"-config", configPath}
	cmd := exec.Command(binPath, args...)

	// 设置日志输出
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// 设置进程属性 - 脱离父进程
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setSysProcAttr(cmd.SysProcAttr)

	// 启动进程
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start %s: %w", name, err)
	}

	// 等待一小段时间确保进程启动成功
	time.Sleep(100 * time.Millisecond)

	// 检查进程是否还在运行
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		logFile.Close()
		return fmt.Errorf("%s failed to start", name)
	}

	logFile.Close()
	return nil
}

// Stop 停止服务
func (m *Manager) Stop(name string) error {
	status := m.getStatus(name)
	if !status.Running {
		return nil // 已经停止
	}

	// 发送 SIGTERM 信号
	process, err := os.FindProcess(status.PID)
	if err != nil {
		return fmt.Errorf("find process: %w", err)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		// 如果 SIGTERM 失败，尝试 SIGKILL
		if err := process.Signal(syscall.SIGKILL); err != nil {
			return fmt.Errorf("kill process: %w", err)
		}
	}

	// 等待进程退出
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		if !pidfile.IsRunningFrom(name, m.pidDir) {
			// 清理 PID 文件
			pidfile.RemoveFrom(name, m.pidDir)
			return nil
		}
	}

	// 超时，强制杀死
	process.Signal(syscall.SIGKILL)
	time.Sleep(100 * time.Millisecond)
	pidfile.RemoveFrom(name, m.pidDir)

	return nil
}

// Restart 重启服务
func (m *Manager) Restart(name string) error {
	if err := m.Stop(name); err != nil {
		// 忽略停止错误（可能本来就没运行）
	}
	time.Sleep(500 * time.Millisecond)
	return m.Start(name)
}

// Status 获取所有服务状态
func (m *Manager) Status() map[string]ServiceStatus {
	return map[string]ServiceStatus{
		"server": m.getStatus("server"),
		"client": m.getStatus("client"),
	}
}

// getStatus 获取服务状态
func (m *Manager) getStatus(name string) ServiceStatus {
	pid, err := pidfile.ReadFrom(name, m.pidDir)
	if err != nil {
		return ServiceStatus{Running: false}
	}

	if pidfile.IsRunningFrom(name, m.pidDir) {
		return ServiceStatus{Running: true, PID: pid}
	}

	// PID 文件存在但进程不存在，清理
	pidfile.RemoveFrom(name, m.pidDir)
	return ServiceStatus{Running: false}
}

// getBinPath 获取二进制路径
func (m *Manager) getBinPath(name string) string {
	binName := "bbgrid-" + name
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	return filepath.Join(m.binDir, binName)
}

// getConfigPath 获取配置路径
func (m *Manager) getConfigPath(name string) string {
	return filepath.Join(m.configDir, name+".json")
}

// ensureDirs 确保目录存在
func (m *Manager) ensureDirs() {
	dirs := []string{
		m.logDir,
		m.configDir,
	}
	for _, dir := range dirs {
		os.MkdirAll(dir, 0755)
	}
}
