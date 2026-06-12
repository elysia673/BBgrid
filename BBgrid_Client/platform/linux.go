//go:build linux

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

type linuxPlatform struct{}

func newPlatform() Platform {
	return &linuxPlatform{}
}

func (p *linuxPlatform) GetListeningPorts() ([]PortInfo, error) {
	out, err := exec.Command("ss", "-tlnp").Output()
	if err != nil {
		out, err = exec.Command("netstat", "-tlnp").Output()
	}
	if err != nil {
		return nil, err
	}
	return parseNetstatOutput(string(out))
}

func (p *linuxPlatform) DetachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}
}

func (p *linuxPlatform) GetDefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".bbgrid"), nil
}

func (p *linuxPlatform) GetExecutableExtension() string {
	return ""
}

func (p *linuxPlatform) GetScriptExtension() string {
	return ".sh"
}

func (p *linuxPlatform) CreateRestartScript(execPath, tmpPath string) error {
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

func (p *linuxPlatform) GetLocalIP() (string, error) {
	return getLocalIPDefault()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func parseNetstatOutput(output string) ([]PortInfo, error) {
	lines := strings.Split(output, "\n")
	var ports []PortInfo

	re := regexp.MustCompile(`(?i)^(tcp|tcp6|udp|udp6)\s+LISTEN\s+\d+\s+\d+\s+([^\s]+):(\d+)\s+[^\s]+(?:\s+users:\(\(\"([^\"]+)\"|\s+(\d+)/(\S+))?`)

	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) < 4 {
			continue
		}
		protocol := strings.ToLower(matches[1])
		if strings.HasPrefix(protocol, "tcp") {
			protocol = "tcp"
		} else if strings.HasPrefix(protocol, "udp") {
			protocol = "udp"
		}
		port, _ := strconv.Atoi(matches[3])
		process := ""
		if len(matches) > 4 && matches[4] != "" {
			process = matches[4]
		} else if len(matches) > 6 && matches[6] != "" {
			process = matches[6]
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
