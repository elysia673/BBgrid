// Package platform 提供跨平台系统操作抽象。
package platform

import (
	"net"
	"os/exec"
)

// Platform 平台操作接口
type Platform interface {
	// GetListeningPorts 获取系统监听端口列表
	GetListeningPorts() ([]PortInfo, error)

	// DetachProcess 脱离终端，以后台模式运行
	DetachProcess(cmd *exec.Cmd)

	// GetDefaultDataDir 获取默认数据目录
	GetDefaultDataDir() (string, error)

	// GetExecutableExtension 获取可执行文件扩展名
	GetExecutableExtension() string

	// GetScriptExtension 获取脚本文件扩展名
	GetScriptExtension() string

	// CreateRestartScript 创建重启脚本
	CreateRestartScript(execPath, tmpPath string) error

	// GetLocalIP 获取本机局域网 IP
	GetLocalIP() (string, error)
}

// PortInfo 端口信息
type PortInfo struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
	Process  string `json:"process,omitempty"`
}

// currentPlatform 当前平台实现
var currentPlatform Platform

func init() {
	currentPlatform = newPlatform()
}

// Get 获取当前平台实例
func Get() Platform {
	return currentPlatform
}

// GetListeningPorts 获取系统监听端口
func GetListeningPorts() ([]PortInfo, error) {
	return currentPlatform.GetListeningPorts()
}

// DetachProcess 脱离终端
func DetachProcess(cmd *exec.Cmd) {
	currentPlatform.DetachProcess(cmd)
}

// GetDefaultDataDir 获取默认数据目录
func GetDefaultDataDir() (string, error) {
	return currentPlatform.GetDefaultDataDir()
}

// GetLocalIP 获取本机局域网 IP
func GetLocalIP() (string, error) {
	return currentPlatform.GetLocalIP()
}

// getLocalIPDefault 默认获取本机 IP 实现
func getLocalIPDefault() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1", nil
	}
	defer conn.Close()
	addr := conn.LocalAddr().(*net.UDPAddr)
	return addr.IP.String(), nil
}
