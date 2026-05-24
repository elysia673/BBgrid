package persist

import "sync"

// StateProvider 需要持久化的模块实现此接口
//
// 核心思想：存期望状态（Desired State），不存运行时状态。
// 模块自己负责 Reconcile，让运行时接近期望状态。
type StateProvider interface {
	// Name 返回 provider 名称，如 "relay", "proxy"
	Name() string

	// Export 返回当前期望状态的深拷贝
	// 调用方保证线程安全（provider 内部加锁）
	Export() any

	// Import 加载期望状态
	// 注意：不是立即恢复，只是加载 desired state
	// 真正恢复由 Reconcile 完成
	Import(data any)
}

// ==================== 全局 Provider 注册表 ====================

var (
	registryMu sync.RWMutex
	registry   = map[string]StateProvider{}
)

// Register 注册一个需要持久化的 provider
//
// 任何模块都可以调用，不需要 import persist 插件。
// 关闭 persist 插件后，注册依然存在，只是没人消费。
func Register(provider StateProvider) {
	registryMu.Lock()
	registry[provider.Name()] = provider
	registryMu.Unlock()
}

// GetAll 获取所有已注册的 provider
func GetAll() map[string]StateProvider {
	registryMu.RLock()
	defer registryMu.RUnlock()
	result := make(map[string]StateProvider, len(registry))
	for k, v := range registry {
		result[k] = v
	}
	return result
}
