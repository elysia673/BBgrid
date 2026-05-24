package main

import (
	"BBgrid/common/model"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

// GetListeningPorts 获取当前系统正在监听的端口列表。
func GetListeningPorts() ([]model.PortInfo, error) {
	switch runtime.GOOS {
	case "linux":
		return getLinuxPorts()
	case "darwin":
		return getDarwinPorts()
	default:
		return nil, fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}
}

// getLinuxPorts 通过 ss/netstat 获取 Linux 系统监听端口。
func getLinuxPorts() ([]model.PortInfo, error) {
	out, err := exec.Command("ss", "-tlnp").Output()
	if err != nil {
		out, err = exec.Command("netstat", "-tlnp").Output()
	}
	if err != nil {
		return nil, err
	}
	return parseNetstatOutput(string(out))
}

// getDarwinPorts 通过 lsof 获取 macOS 系统监听端口。
func getDarwinPorts() ([]model.PortInfo, error) {
	out, err := exec.Command("lsof", "-nP", "-iTCP", "-sTCP:LISTEN").Output()
	if err != nil {
		return nil, err
	}
	return parseLsofOutput(string(out))
}

// parseNetstatOutput 解析 ss/netstat 输出格式。
func parseNetstatOutput(output string) ([]model.PortInfo, error) {
	lines := strings.Split(output, "\n")
	var ports []model.PortInfo

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
		ports = append(ports, model.PortInfo{
			Protocol: protocol,
			Port:     port,
			Process:  process,
		})
	}
	return DeduplicatePorts(ports), nil
}

// parseLsofOutput 解析 lsof 输出格式。
func parseLsofOutput(output string) ([]model.PortInfo, error) {
	lines := strings.Split(output, "\n")
	portsMap := make(map[int]model.PortInfo)

	re := regexp.MustCompile(`^(\S+)\s+\d+\s+\S+\s+\S+\s+IPv[46]\s+\S+\s+\S+\s+TCP\s+(\S+):(\d+)\s+\(LISTEN\)`)
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 4 {
			process := matches[1]
			port, _ := strconv.Atoi(matches[3])

			if existing, ok := portsMap[port]; !ok {
				portsMap[port] = model.PortInfo{
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

	ports := make([]model.PortInfo, 0, len(portsMap))
	for _, p := range portsMap {
		ports = append(ports, p)
	}
	return ports, nil
}
