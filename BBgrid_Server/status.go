package main

import (
	"BBgrid/common/plugin"
	"fmt"
	"time"
)

// ServerStatus 服务器状态
type ServerStatus struct {
	StartTime  time.Time   `json:"start_time"`
	Uptime     string      `json:"uptime"`
	Version    string      `json:"version"`
	PublicIP   string      `json:"public_ip"`
	Components Components  `json:"components"`
	Stats      Stats       `json:"stats"`
	Plugins    PluginsInfo `json:"plugins"`
}

// Components 组件状态
type Components struct {
	Auth    ComponentStatus `json:"auth"`
	State   ComponentStatus `json:"state"`
	Data    ComponentStatus `json:"data"`
	Control ComponentStatus `json:"control"`
	WS      ComponentStatus `json:"ws"`
	Tunnel  ComponentStatus `json:"tunnel"`
}

// ComponentStatus 组件状态
type ComponentStatus struct {
	Status string `json:"status"` // ok, error, starting
	Error  string `json:"error,omitempty"`
	Uptime string `json:"uptime,omitempty"`
}

// Stats 统计信息
type Stats struct {
	Clients int `json:"clients"`
	Proxies int `json:"proxies"`
	Relays  int `json:"relays"`
}

// PluginsInfo 插件信息
type PluginsInfo struct {
	Count  int          `json:"count"`
	Static []PluginInfo `json:"static"`
}

// PluginInfo 单个插件信息
type PluginInfo struct {
	Name    string          `json:"name"`
	Version string          `json:"version"`
	Actions []plugin.Action `json:"actions,omitempty"`
}

// StatusCollector 状态收集器
type StatusCollector struct {
	startTime time.Time
	version   string
	publicIP  string

	// 组件状态
	auth    ComponentStatus
	state   ComponentStatus
	data    ComponentStatus
	control ComponentStatus
	ws      ComponentStatus
	tunnel  ComponentStatus

	// 统计信息
	clients int
	proxies int
	relays  int

	// 插件信息
	plugins []plugin.Plugin
}

// NewStatusCollector 创建状态收集器
func NewStatusCollector(version, publicIP string) *StatusCollector {
	return &StatusCollector{
		startTime: time.Now(),
		version:   version,
		publicIP:  publicIP,
		auth:      ComponentStatus{Status: "starting"},
		state:     ComponentStatus{Status: "starting"},
		data:      ComponentStatus{Status: "starting"},
		control:   ComponentStatus{Status: "starting"},
		ws:        ComponentStatus{Status: "starting"},
		tunnel:    ComponentStatus{Status: "starting"},
	}
}

// SetAuthStatus 设置认证组件状态
func (sc *StatusCollector) SetAuthStatus(status string, err string) {
	sc.auth = ComponentStatus{Status: status, Error: err}
}

// SetStateStatus 设置状态机组件状态
func (sc *StatusCollector) SetStateStatus(status string, err string) {
	sc.state = ComponentStatus{Status: status, Error: err}
}

// SetDataStatus 设置数据面组件状态
func (sc *StatusCollector) SetDataStatus(status string, err string) {
	sc.data = ComponentStatus{Status: status, Error: err}
}

// SetControlStatus 设置控制中心组件状态
func (sc *StatusCollector) SetControlStatus(status string, err string) {
	sc.control = ComponentStatus{Status: status, Error: err}
}

// SetWSStatus 设置 WebSocket 组件状态
func (sc *StatusCollector) SetWSStatus(status string, err string) {
	sc.ws = ComponentStatus{Status: status, Error: err}
}

// SetTunnelStatus 设置隧道组件状态
func (sc *StatusCollector) SetTunnelStatus(status string, err string) {
	sc.tunnel = ComponentStatus{Status: status, Error: err}
}

// UpdateStats 更新统计信息
func (sc *StatusCollector) UpdateStats(clients, proxies, relays int) {
	sc.clients = clients
	sc.proxies = proxies
	sc.relays = relays
}

// SetPlugins 设置插件列表
func (sc *StatusCollector) SetPlugins(plugins []plugin.Plugin) {
	sc.plugins = plugins
}

// GetStatus 获取服务器状态
func (sc *StatusCollector) GetStatus() ServerStatus {
	uptime := time.Since(sc.startTime)

	return ServerStatus{
		StartTime: sc.startTime,
		Uptime:    formatDuration(uptime),
		Version:   sc.version,
		PublicIP:  sc.publicIP,
		Components: Components{
			Auth:    sc.addUptime(sc.auth, sc.startTime),
			State:   sc.addUptime(sc.state, sc.startTime),
			Data:    sc.addUptime(sc.data, sc.startTime),
			Control: sc.addUptime(sc.control, sc.startTime),
			WS:      sc.addUptime(sc.ws, sc.startTime),
			Tunnel:  sc.addUptime(sc.tunnel, sc.startTime),
		},
		Stats: Stats{
			Clients: sc.clients,
			Proxies: sc.proxies,
			Relays:  sc.relays,
		},
		Plugins: sc.getPluginsInfo(),
	}
}

// addUptime 添加运行时间
func (sc *StatusCollector) addUptime(cs ComponentStatus, startTime time.Time) ComponentStatus {
	if cs.Status == "ok" {
		cs.Uptime = formatDuration(time.Since(startTime))
	}
	return cs
}

// getPluginsInfo 获取插件信息
func (sc *StatusCollector) getPluginsInfo() PluginsInfo {
	staticPlugins := make([]PluginInfo, 0, len(sc.plugins))
	for _, p := range sc.plugins {
		staticPlugins = append(staticPlugins, PluginInfo{
			Name:    p.Name(),
			Version: p.Version(),
			Actions: p.Actions(),
		})
	}

	return PluginsInfo{
		Count:  len(staticPlugins),
		Static: staticPlugins,
	}
}

// formatDuration 格式化持续时间
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return d.Truncate(time.Second).String()
	}
	if d < time.Hour {
		return d.Truncate(time.Minute).String()
	}
	if d < 24*time.Hour {
		return d.Truncate(time.Hour).String()
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, hours)
}
