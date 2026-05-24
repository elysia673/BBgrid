// Package plugin 提供插件接口定义和注册机制
//
// 架构设计：
//   - Plugin: 声明能力（Action）和资源能力（Capability）
//   - Server: 自动注册 Capability，动态路由事件
//   - CLI:    Action → 用户输入
package plugin

import (
	"BBgrid/common/proto"
	"sync"
)

// Dispatcher 事件分发器接口
//
// 插件通过此接口订阅和发布事件。
type Dispatcher interface {
	SubscribeByType(resourceType string, handler func(proto.GenericEvent))
	Dispatch(event proto.GenericEvent)
}

// StateStore 状态存储接口
type StateStore interface {
	// 根据需要扩展
}

// Plugin 插件接口
//
// 插件声明能力（Action）和资源能力（Capability），
// Server 自动注册和路由。
type Plugin interface {
	// Name 返回插件名称
	Name() string

	// Version 返回插件版本
	Version() string

	// Init 初始化插件
	Init(dispatcher Dispatcher, state StateStore, config map[string]any) error

	// Run 运行插件
	Run() error

	// Stop 停止插件
	Stop()

	// Actions 返回插件声明的能力列表
	//
	// 只声明"能做什么"，不关心"怎么做"。
	Actions() []Action

	// Capabilities 返回插件声明的资源能力列表
	//
	// 声明"我能处理什么资源"，Server 自动注册和路由。
	// 返回 nil 表示不处理任何资源（纯 Action 插件）。
	Capabilities() []Capability
}

// Action 能力声明
type Action struct {
	Name        string  `json:"name"`                  // 能力名称，如 "latency.get"
	Description string  `json:"description,omitempty"` // 能力描述
	Params      []Param `json:"params,omitempty"`      // 参数列表
}

// Param 参数定义
type Param struct {
	Name        string `json:"name"`                  // 参数名称
	Type        string `json:"type"`                  // 参数类型：string, int, bool
	Required    bool   `json:"required,omitempty"`    // 是否必填
	Default     string `json:"default,omitempty"`     // 默认值
	Description string `json:"description,omitempty"` // 参数描述
}

// Capability 资源能力声明
//
// 声明插件能处理什么类型的资源。
// Server 根据此声明自动路由 GenericEvent 到插件。
type Capability struct {
	// ResourceType 资源类型
	//
	// 如 "proxy", "relay", "client", "namespace"
	ResourceType string `json:"resource_type"`

	// EventTypes 事件类型列表
	//
	// 如 ["ADDED", "MODIFIED", "DELETED"]
	// 空列表表示监听所有事件类型。
	EventTypes []proto.EventType `json:"event_types,omitempty"`

	// Description 能力描述
	Description string `json:"description,omitempty"`
}

// PluginConfig 插件配置
type PluginConfig struct {
	Enabled bool           `json:"enabled"`
	Config  map[string]any `json:"config"`
}

// SyncResponse 同步响应
type SyncResponse struct {
	Actions      []Action     `json:"actions"`
	Capabilities []Capability `json:"capabilities,omitempty"`
}

// ==================== 注册表 ====================

var (
	mu       sync.RWMutex
	registry = map[string]func() Plugin{}
)

// Register 注册插件工厂函数
func Register(name string, factory func() Plugin) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = factory
}

// GetAll 获取所有已注册的插件工厂函数
func GetAll() map[string]func() Plugin {
	mu.RLock()
	defer mu.RUnlock()
	result := make(map[string]func() Plugin)
	for k, v := range registry {
		result[k] = v
	}
	return result
}

// Get 获取指定插件的工厂函数
func Get(name string) (func() Plugin, bool) {
	mu.RLock()
	defer mu.RUnlock()
	factory, ok := registry[name]
	return factory, ok
}
