// Package pidfile 提供 PID 文件管理功能。
package pidfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// DefaultDir 默认 PID 文件目录
const DefaultDir = "/var/run/bbgrid"

// Write 写入 PID 文件
func Write(name string) error {
	return WriteTo(name, DefaultDir)
}

// WriteTo 写入 PID 文件到指定目录
func WriteTo(name, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create pid dir: %w", err)
	}

	pidPath := filepath.Join(dir, name+".pid")
	pid := os.Getpid()

	return os.WriteFile(pidPath, []byte(strconv.Itoa(pid)), 0644)
}

// Remove 删除 PID 文件
func Remove(name string) error {
	return RemoveFrom(name, DefaultDir)
}

// RemoveFrom 从指定目录删除 PID 文件
func RemoveFrom(name, dir string) error {
	pidPath := filepath.Join(dir, name+".pid")
	err := os.Remove(pidPath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// Read 读取 PID 文件
func Read(name string) (int, error) {
	return ReadFrom(name, DefaultDir)
}

// ReadFrom 从指定目录读取 PID 文件
func ReadFrom(name, dir string) (int, error) {
	pidPath := filepath.Join(dir, name+".pid")
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid pid file: %w", err)
	}

	return pid, nil
}

// IsRunning 检查进程是否运行
func IsRunning(name string) bool {
	return IsRunningFrom(name, DefaultDir)
}

// IsRunningFrom 从指定目录检查进程是否运行
func IsRunningFrom(name, dir string) bool {
	pid, err := ReadFrom(name, dir)
	if err != nil {
		return false
	}

	return isProcessAlive(pid)
}

// isProcessAlive 检查进程是否存在
func isProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	err = process.Signal(syscall.Signal(0))
	return err == nil
}
