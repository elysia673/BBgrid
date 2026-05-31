package runtime

import (
	"sync"
)

// CapabilityRegistryImpl 能力注册表实现
type CapabilityRegistryImpl struct {
	mu             sync.RWMutex
	entries        map[string]capabilityEntry
	proxyProviders map[string]ProxyProvider
	relayProviders map[string]RelayProvider
}

type capabilityEntry struct {
	capability Capability
	handler    ActionHandler
}

// NewCapabilityRegistry 创建能力注册表
func NewCapabilityRegistry() *CapabilityRegistryImpl {
	return &CapabilityRegistryImpl{
		entries:        make(map[string]capabilityEntry),
		proxyProviders: make(map[string]ProxyProvider),
		relayProviders: make(map[string]RelayProvider),
	}
}

// Register 注册能力
func (r *CapabilityRegistryImpl) Register(capability Capability, handler ActionHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[capability.Name] = capabilityEntry{
		capability: capability,
		handler:    handler,
	}
}

// Unregister 注销能力
func (r *CapabilityRegistryImpl) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, name)
}

// Get 获取能力
func (r *CapabilityRegistryImpl) Get(name string) (Capability, ActionHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[name]
	if !ok {
		return Capability{}, nil, false
	}
	return entry.capability, entry.handler, true
}

// List 列出所有能力
func (r *CapabilityRegistryImpl) List() []Capability {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Capability, 0, len(r.entries))
	for _, entry := range r.entries {
		result = append(result, entry.capability)
	}
	return result
}

// Has 检查能力是否存在
func (r *CapabilityRegistryImpl) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.entries[name]
	return ok
}

// Execute 执行 Action
func (r *CapabilityRegistryImpl) Execute(ctx *ActionContext) (*ActionResult, error) {
	r.mu.RLock()
	entry, ok := r.entries[ctx.Action]
	r.mu.RUnlock()
	if !ok {
		return &ActionResult{Code: 404, Msg: "action not found: " + ctx.Action}, nil
	}
	return entry.handler(ctx)
}

// RegisterProxyProvider 注册代理提供者
func (r *CapabilityRegistryImpl) RegisterProxyProvider(name string, provider ProxyProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.proxyProviders[name] = provider
}

// GetProxyProvider 获取代理提供者
func (r *CapabilityRegistryImpl) GetProxyProvider(name string) (ProxyProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.proxyProviders[name]
	return p, ok
}

// RegisterRelayProvider 注册中继提供者
func (r *CapabilityRegistryImpl) RegisterRelayProvider(name string, provider RelayProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.relayProviders[name] = provider
}

// GetRelayProvider 获取中继提供者
func (r *CapabilityRegistryImpl) GetRelayProvider(name string) (RelayProvider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.relayProviders[name]
	return p, ok
}
