package runtime

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"BBgrid/common/store"
	"sync/atomic"
	"time"
)

// Core Runtime Core 编排器
//
// 负责:
// - EventBus (事件路由)
// - StateStore (状态存储, event-driven apply)
// - ReconcileEngine (状态协调)
// - CapabilityRegistry (能力注册)
//
// 禁止:
// - 直接写 StateStore
// - 直接修改状态
type Core struct {
	eventBus   *EventBusImpl
	stateStore *StateStoreImpl
	reconcile  *ReconcileEngine
	capability *CapabilityRegistryImpl
	storage    *store.StorageManager
	restoring  atomic.Bool
}

// CoreConfig Runtime Core 配置
type CoreConfig struct {
	PublicIP          string
	ReconcileInterval int // 秒
}

// NewCore 创建 Runtime Core
func NewCore(config CoreConfig, storage *store.StorageManager) *Core {
	eventBus := NewEventBus()
	stateStore := NewStateStore(config.PublicIP)
	capability := NewCapabilityRegistry()

	reconcileConfig := DefaultReconcileConfig()
	if config.ReconcileInterval > 0 {
		reconcileConfig.Interval = intToDuration(config.ReconcileInterval)
	}

	core := &Core{
		eventBus:   eventBus,
		stateStore: stateStore,
		capability: capability,
		storage:    storage,
	}

	// 创建 ReconcileProvider (通过 EventBus 创建资源)
	provider := NewEventBusReconcileProvider(core)

	// 创建 ReconcileEngine
	core.reconcile = NewReconcileEngine(reconcileConfig, stateStore, provider)

	// 订阅事件到 StateStore
	eventBus.Subscribe(proto.ResourceTypeClient, func(event proto.GenericEvent) {
		stateStore.Apply(event)
	})
	eventBus.Subscribe(proto.ResourceTypeProxy, func(event proto.GenericEvent) {
		stateStore.Apply(event)
		// 持久化（恢复模式跳过，避免覆盖 MetaStore 原始数据）
		if storage != nil && !core.restoring.Load() {
			if event.EventType == proto.EventAdded {
				if err := storage.AppendEventAndSave(event, event.Payload); err != nil {
					alog.Error(alog.CatSystem, "proxy 持久化失败", "error", err, "event_id", event.ID)
				}
			} else if event.EventType == proto.EventDeleted {
				alog.Info(alog.CatSystem, "proxy 删除持久化", "resource", event.Resource.String(), "event_id", event.ID)
				if err := storage.DeleteResource(event.Resource); err != nil {
					alog.Error(alog.CatSystem, "proxy 删除持久化失败", "error", err, "event_id", event.ID)
				}
			}
		}
		// 触发 ReconcileEngine
		if core.reconcile != nil {
			core.reconcile.Trigger()
		}
	})
	eventBus.Subscribe(proto.ResourceTypeRelay, func(event proto.GenericEvent) {
		stateStore.Apply(event)
		if storage != nil && !core.restoring.Load() {
			if event.EventType == proto.EventAdded {
				if err := storage.AppendEventAndSave(event, event.Payload); err != nil {
					alog.Error(alog.CatSystem, "relay 持久化失败", "error", err, "event_id", event.ID)
				}
			} else if event.EventType == proto.EventDeleted {
				if err := storage.DeleteResource(event.Resource); err != nil {
					alog.Error(alog.CatSystem, "relay 删除持久化失败", "error", err, "event_id", event.ID)
				}
			}
		}
		// 触发 ReconcileEngine
		if core.reconcile != nil {
			core.reconcile.Trigger()
		}
	})

	return core
}

// EventBus 获取事件总线
func (c *Core) EventBus() EventBus {
	return c.eventBus
}

// StateStore 获取状态存储
func (c *Core) StateStore() StateStore {
	return c.stateStore
}

// Reconcile 获取协调引擎
func (c *Core) Reconcile() *ReconcileEngine {
	return c.reconcile
}

// Capability 获取能力注册表
func (c *Core) Capability() CapabilityRegistry {
	return c.capability
}

// Storage 获取存储管理器 (供插件使用)
func (c *Core) Storage() *store.StorageManager {
	return c.storage
}

// SetReconcileProvider 设置协调提供者
func (c *Core) SetReconcileProvider(provider ReconcileProvider) {
	// 先停掉旧的 ReconcileEngine，避免旧 goroutine 泄露
	c.reconcile.Stop()
	c.reconcile = NewReconcileEngine(c.reconcile.config, c.stateStore, provider)
}

// Publish 发布事件 (快捷方式)
func (c *Core) Publish(event proto.GenericEvent) {
	c.eventBus.Publish(event)
}

// Start 启动 Runtime Core
func (c *Core) Start() {
	alog.Info(alog.CatSystem, "Runtime Core 启动")
	// 启动协调引擎
	go func() {
		if err := c.reconcile.Run(); err != nil {
			alog.Error(alog.CatSystem, "ReconcileEngine 异常退出", "error", err)
		}
	}()
}

// SetRestoring 设置恢复模式标志
func (c *Core) SetRestoring(v bool) {
	c.restoring.Store(v)
}

// IsRestoring 是否处于恢复模式
func (c *Core) IsRestoring() bool {
	return c.restoring.Load()
}

// Stop 停止 Runtime Core
func (c *Core) Stop() {
	alog.Info(alog.CatSystem, "Runtime Core 停止")
	c.reconcile.Stop()
	c.eventBus.Close()
}

// 辅助函数
func intToDuration(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}
