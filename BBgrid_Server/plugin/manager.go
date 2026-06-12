// Package plugin 提供插件管理抽象层。
package plugin

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"fmt"
	"sync"
)

// Plugin 插件接口 (保持向后兼容)
type Plugin interface {
	// Name 返回插件名称
	Name() string

	// Version 返回插件版本
	Version() string

	// Init 初始化插件
	Init(core *runtime.Core, config map[string]any) error

	// Run 运行插件 (阻塞)
	Run() error

	// Stop 停止插件
	Stop()
}

// Factory 插件工厂函数
type Factory func() Plugin

// Manager 插件管理器
type Manager struct {
	mu          sync.RWMutex
	registry    map[string]Factory
	plugins     map[string]Plugin
	core        *runtime.Core
	globalConfig map[string]any
}

// NewManager 创建插件管理器
func NewManager(core *runtime.Core, globalConfig map[string]any) *Manager {
	return &Manager{
		registry:     make(map[string]Factory),
		plugins:      make(map[string]Plugin),
		core:         core,
		globalConfig: globalConfig,
	}
}

// Register 注册插件工厂
func (m *Manager) Register(name string, factory Factory) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registry[name] = factory
}

// InitAll 初始化所有插件
// enabled: map[pluginName]map[configKey]configValue
func (m *Manager) InitAll(enabled map[string]map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, factory := range m.registry {
		// 检查是否启用
		if enabled != nil {
			if cfg, ok := enabled[name]; ok {
				if enabledVal, ok := cfg["enabled"]; ok {
					if b, ok := enabledVal.(bool); ok && !b {
						alog.Info(alog.CatSystem, "插件已禁用", "name", name)
						continue
					}
				}
			}
		}

		// 创建插件
		p := factory()

		// 合并配置: 全局配置 + 插件专属配置
		finalConfig := make(map[string]any, len(m.globalConfig))
		for k, v := range m.globalConfig {
			finalConfig[k] = v
		}
		if enabled != nil {
			if cfg, ok := enabled[name]; ok {
				for k, v := range cfg {
					finalConfig[k] = v
				}
			}
		}

		// 初始化
		if err := p.Init(m.core, finalConfig); err != nil {
			alog.Error(alog.CatSystem, "插件初始化失败", "name", name, "error", err)
			continue
		}

		m.plugins[name] = p
		alog.Info(alog.CatSystem, "插件已初始化", "name", name, "version", p.Version())
	}
}

// StartAll 启动所有已初始化的插件
func (m *Manager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, p := range m.plugins {
		go func(n string, plugin Plugin) {
			if err := plugin.Run(); err != nil {
				alog.Error(alog.CatSystem, "插件异常退出", "name", n, "error", err)
			}
		}(name, p)

		alog.Info(alog.CatSystem, "插件已启动", "name", name)
	}
}

// StopAll 停止所有插件
func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, p := range m.plugins {
		p.Stop()
		alog.Info(alog.CatSystem, "插件已停止", "name", name)
	}
	m.plugins = make(map[string]Plugin)
}

// Get 获取已加载的插件
func (m *Manager) Get(name string) (Plugin, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.plugins[name]
	return p, ok
}

// List 列出所有已加载的插件
func (m *Manager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.plugins))
	for name := range m.plugins {
		names = append(names, name)
	}
	return names
}

// Registered 列出所有已注册的插件
func (m *Manager) Registered() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.registry))
	for name := range m.registry {
		names = append(names, name)
	}
	return names
}

// Error 插件错误
type Error struct {
	Plugin  string
	Action  string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("plugin %s: %s: %s", e.Plugin, e.Action, e.Message)
}

// NewError 创建插件错误
func NewError(plugin, action, message string) *Error {
	return &Error{
		Plugin:  plugin,
		Action:  action,
		Message: message,
	}
}

// GetConfigString 从配置中获取字符串值
func GetConfigString(config map[string]any, key, defaultVal string) string {
	if val, ok := config[key].(string); ok && val != "" {
		return val
	}
	return defaultVal
}

// GetConfigInt 从配置中获取整数值
func GetConfigInt(config map[string]any, key string, defaultVal int) int {
	if val, ok := config[key].(float64); ok {
		return int(val)
	}
	return defaultVal
}

// GetConfigBool 从配置中获取布尔值
func GetConfigBool(config map[string]any, key string, defaultVal bool) bool {
	if val, ok := config[key].(bool); ok {
		return val
	}
	return defaultVal
}
