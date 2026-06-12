//go:build windows

package platform

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

type windowsPlatform struct{}

func newPlatform() Platform {
	return &windowsPlatform{}
}

func (p *windowsPlatform) GetListeningPorts() ([]PortInfo, error) {
	out, err := exec.Command("netstat", "-ano").Output()
	if err != nil {
		return nil, err
	}
	return parseWindowsNetstatOutput(string(out))
}

func (p *windowsPlatform) DetachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x08000000, // DETACHED_PROCESS
		HideWindow:    true,
	}
}

func (p *windowsPlatform) GetDefaultDataDir() (string, error) {
	appData := os.Getenv("APPDATA")
	if appData == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, "AppData", "Roaming", "BBgrid"), nil
	}
	return filepath.Join(appData, "BBgrid"), nil
}

func (p *windowsPlatform) GetExecutableExtension() string {
	return ".exe"
}

func (p *windowsPlatform) GetScriptExtension() string {
	return ".bat"
}

func (p *windowsPlatform) CreateRestartScript(execPath, tmpPath string) error {
	execDir := filepath.Dir(execPath)
	scriptPath := filepath.Join(execDir, "bbgrid_restart.bat")

	scriptContent := fmt.Sprintf(`@echo off
timeout /t 2 /nobreak >nul
move /y "%s" "%s"
start "" "%s"
del "%%~f0"
`, filepath.Clean(tmpPath), filepath.Clean(execPath), filepath.Clean(execPath))

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		return err
	}

	go func() {
		cmd := exec.Command("cmd", "/c", "start", "/b", scriptPath)
		cmd.Start()
	}()

	return nil
}

func (p *windowsPlatform) GetLocalIP() (string, error) {
	return getLocalIPDefault()
}

func parseWindowsNetstatOutput(output string) ([]PortInfo, error) {
	lines := strings.Split(output, "\n")
	var ports []PortInfo

	re := regexp.MustCompile(`^\s*(TCP|UDP)\s+(\S+):(\d+)\s+\S+(?:\s+(\S+))?\s*`)

	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) < 4 {
			continue
		}

		protocol := strings.ToLower(matches[1])
		addr := matches[2]
		portStr := matches[3]
		pidStr := ""
		if len(matches) > 4 {
			pidStr = matches[4]
		}

		// 只处理 LISTEN 状态的 TCP 连接
		if protocol == "tcp" && !strings.Contains(line, "LISTENING") {
			continue
		}

		// 跳过非本地地址
		if addr != "0.0.0.0" && addr != "127.0.0.1" && addr != "[::]" && addr != "::" {
			continue
		}

		port, _ := strconv.Atoi(portStr)
		process := ""
		if pidStr != "" {
			process = fmt.Sprintf("PID:%s", pidStr)
		}

		ports = append(ports, PortInfo{
			Protocol: protocol,
			Port:     port,
			Process:  process,
		})
	}
	return deduplicatePorts(ports), nil
}

func deduplicatePorts(ports []PortInfo) []PortInfo {
	seen := make(map[int]bool)
	result := make([]PortInfo, 0, len(ports))
	for _, p := range ports {
		if !seen[p.Port] {
			seen[p.Port] = true
			result = append(result, p)
		}
	}
	return result
}
