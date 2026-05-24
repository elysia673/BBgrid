package main

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"fmt"
	"sync"
)

// Dispatcher 通用事件分发器
//
// 基于 ResourceKey 的事件分发，完全不理解业务逻辑。
// 只做一件事：根据资源类型路由事件到对应的 handler。
type Dispatcher struct {
	mu sync.RWMutex
	// 按资源类型订阅 (如 "proxy", "client", "relay")
	typeSubs map[string][]func(proto.GenericEvent)
	closed   bool
}

// NewDispatcher 创建通用事件分发器
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		typeSubs: make(map[string][]func(proto.GenericEvent)),
	}
}

// SubscribeByType 按资源类型订阅事件
func (d *Dispatcher) SubscribeByType(resourceType string, handler func(proto.GenericEvent)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.typeSubs[resourceType] = append(d.typeSubs[resourceType], handler)
}

// UnsubscribeByType 取消按资源类型的事件订阅
func (d *Dispatcher) UnsubscribeByType(resourceType string, handler func(proto.GenericEvent)) {
	d.mu.Lock()
	defer d.mu.Unlock()

	handlers := d.typeSubs[resourceType]
	target := fmt.Sprintf("%p", handler)
	for i, h := range handlers {
		if fmt.Sprintf("%p", h) == target {
			d.typeSubs[resourceType] = append(handlers[:i], handlers[i+1:]...)
			return
		}
	}
}

// Dispatch 分发事件 (按资源类型路由)
//
// 异步触发所有订阅者，不阻塞调用方。
func (d *Dispatcher) Dispatch(event proto.GenericEvent) {
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return
	}

	// 复制 handlers 避免持锁调用
	handlers := make([]func(proto.GenericEvent), len(d.typeSubs[event.Resource.Type]))
	copy(handlers, d.typeSubs[event.Resource.Type])
	d.mu.RUnlock()

	// 异步触发所有 handler（带 panic 恢复）
	for _, h := range handlers {
		go func(handler func(proto.GenericEvent)) {
			defer func() {
				if r := recover(); r != nil {
					alog.Error(alog.CatServer, "handler panic", "error", r)
				}
			}()
			handler(event)
		}(h)
	}
}

// Close 关闭分发器
func (d *Dispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.typeSubs = make(map[string][]func(proto.GenericEvent))
}
