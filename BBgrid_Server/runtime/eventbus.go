package runtime

import (
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"fmt"
	"sync"
	"sync/atomic"
)

// EventBusImpl 事件总线实现
//
// 增强版 Dispatcher:
// - 支持有序投递 (sequence number)
// - 支持同步/异步模式
// - 保持向后兼容
type EventBusImpl struct {
	mu       sync.RWMutex
	typeSubs map[string][]EventHandler
	closed   bool
	seq      int64

	queues    map[string]chan proto.GenericEvent
	queueMu   sync.Mutex
	wg        sync.WaitGroup
	stopped   atomic.Bool
}

// NewEventBus 创建事件总线
func NewEventBus() *EventBusImpl {
	return &EventBusImpl{
		typeSubs: make(map[string][]EventHandler),
		queues:   make(map[string]chan proto.GenericEvent),
	}
}

// Publish 发布事件到总线
//
// 事件按 resourceType 路由到所有匹配的订阅者。
// 同一资源类型的事件按序列号顺序处理，保证有序投递。
func (b *EventBusImpl) Publish(event proto.GenericEvent) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}

	// 分配序列号
	b.seq++
	event.Sequence = b.seq

	// 检查是否有订阅者
	_, hasSubscribers := b.typeSubs[event.Resource.Type]
	subCount := len(b.typeSubs[event.Resource.Type])
	b.mu.Unlock()

	if !hasSubscribers {
		alog.Warn(alog.CatSystem, "EventBus 无订阅者，丢弃事件", "type", event.Resource.Type, "name", event.Resource.Name)
		return
	}

	alog.Info(alog.CatSystem, "EventBus 发布事件", "type", event.Resource.Type, "event", string(event.EventType), "name", event.Resource.Name, "subs", subCount)

	// 获取或创建队列
	b.queueMu.Lock()
	queue, exists := b.queues[event.Resource.Type]
	if !exists {
		if b.stopped.Load() {
			b.queueMu.Unlock()
			return
		}
		queue = make(chan proto.GenericEvent, 256)
		b.queues[event.Resource.Type] = queue
		b.wg.Add(1)
		go b.worker(event.Resource.Type, queue)
	}
	b.queueMu.Unlock()

	// 将事件放入队列（recover 防止 Close() 并发关闭 channel 导致 panic）
	func() {
		defer func() {
			if r := recover(); r != nil {
				alog.Warn(alog.CatSystem, "EventBus 队列已关闭，丢弃事件",
					"resource_type", event.Resource.Type,
					"event_type", event.EventType,
				)
			}
		}()
		select {
		case queue <- event:
		default:
			alog.Warn(alog.CatSystem, "EventBus 队列已满，丢弃事件",
				"resource_type", event.Resource.Type,
				"event_type", event.EventType,
			)
		}
	}()
}

// Subscribe 按资源类型订阅事件
func (b *EventBusImpl) Subscribe(resourceType string, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.typeSubs[resourceType] = append(b.typeSubs[resourceType], handler)
}

// Unsubscribe 取消订阅
func (b *EventBusImpl) Unsubscribe(resourceType string, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()

	handlers := b.typeSubs[resourceType]
	for i, h := range handlers {
		// 比较函数指针
		if fmt.Sprintf("%p", h) == fmt.Sprintf("%p", handler) {
			b.typeSubs[resourceType] = append(handlers[:i], handlers[i+1:]...)
			return
		}
	}
}

// worker 处理队列中的事件
func (b *EventBusImpl) worker(resourceType string, queue chan proto.GenericEvent) {
	defer b.wg.Done()

	for event := range queue {
		b.mu.RLock()
		handlers := make([]EventHandler, len(b.typeSubs[resourceType]))
		copy(handlers, b.typeSubs[resourceType])
		b.mu.RUnlock()

		if len(handlers) == 0 {
			alog.Warn(alog.CatSystem, "EventBus 无 handler 处理事件", "type", resourceType, "event", event.EventType)
			continue
		}
		for _, handler := range handlers {
			func() {
				defer func() {
					if r := recover(); r != nil {
						alog.Error(alog.CatSystem, "EventBus handler panic",
							"resource", event.Resource.String(),
							"event_type", event.EventType,
							"panic", r,
						)
					}
				}()
				handler(event)
			}()
		}
	}
}

// Close 关闭事件总线
func (b *EventBusImpl) Close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()

	b.stopped.Store(true)

	b.queueMu.Lock()
	for _, queue := range b.queues {
		close(queue)
	}
	b.queues = make(map[string]chan proto.GenericEvent)
	b.queueMu.Unlock()

	// 等待所有 worker 完成（worker 会排空队列中剩余事件）
	b.wg.Wait()

	// worker 全部退出后再清空订阅，避免 worker 排空时 handler 丢失
	b.mu.Lock()
	b.typeSubs = make(map[string][]EventHandler)
	b.mu.Unlock()
}

// Seq 返回当前序列号
func (b *EventBusImpl) Seq() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.seq
}
