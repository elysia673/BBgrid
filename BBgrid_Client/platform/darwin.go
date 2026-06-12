//go:build darwin

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

type darwinPlatform struct{}

func newPlatform() Platform {
	return &darwinPlatform{}
}

func (p *darwinPlatform) GetListeningPorts() ([]PortInfo, error) {
	out, err := exec.Command("lsof", "-nP", "-iTCP", "-sTCP:LISTEN").Output()
	if err != nil {
		return nil, err
	}
	return parseLsofOutput(string(out))
}

func (p *darwinPlatform) DetachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}

func (p *darwinPlatform) GetDefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".bbgrid"), nil
}

func (p *darwinPlatform) GetExecutableExtension() string {
	return ""
}

func (p *darwinPlatform) GetScriptExtension() string {
	return ".sh"
}

func (p *darwinPlatform) CreateRestartScript(execPath, tmpPath string) error {
	execDir := filepath.Dir(execPath)
	scriptPath := filepath.Join(execDir, "bbgrid_restart.sh")

	scriptContent := fmt.Sprintf(`#!/bin/sh
sleep 2
mv %s %s
chmod +x %s
exec %s &
rm -- "$0"
`, shellQuote(tmpPath), shellQuote(execPath), shellQuote(execPath), shellQuote(execPath))

	if err := os.WriteFile(scriptPath, []byte(scriptContent), 0755); err != nil {
		return err
	}

	go func() {
		cmd := exec.Command("/bin/sh", scriptPath)
		cmd.Start()
	}()

	return nil
}

func (p *darwinPlatform) GetLocalIP() (string, error) {
	return getLocalIPDefault()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func parseLsofOutput(output string) ([]PortInfo, error) {
	lines := strings.Split(output, "\n")
	portsMap := make(map[int]PortInfo)

	re := regexp.MustCompile(`^(\S+)\s+\d+\s+\S+\s+\S+\s+IPv[46]\s+\S+\s+\S+\s+TCP\s+(\S+):(\d+)\s+\(LISTEN\)`)
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 4 {
			process := matches[1]
			port, _ := strconv.Atoi(matches[3])

			if existing, ok := portsMap[port]; !ok {
				portsMap[port] = PortInfo{
					Protocol: "tcp",
					Port:     port,
					Process:  process,
				}
			} else if existing.Process == "" && process != "" {
				existing.Process = process
				portsMap[port] = existing
			}
		}
	}

	ports := make([]PortInfo, 0, len(portsMap))
	for _, p := range portsMap {
		ports = append(ports, p)
	}
	return ports, nil
}
