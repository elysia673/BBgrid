package store

import (
	"BBgrid/common/proto"
	"sync"
)

// ==================== 存储管理器 ====================

// StorageManager 存储管理器
//
// 统一管理事件存储、快照存储、元数据存储。
type StorageManager struct {
	mu        sync.RWMutex
	events    *EventStore
	snapshots *SnapshotStore
	meta      *MetaStore

	// 快照配置
	snapshotInterval int64 // 快照间隔（事件数量）
	eventCount       int64 // 自上次快照以来的事件数量
}

// StorageConfig 存储配置
type StorageConfig struct {
	DataDir          string // 数据目录
	SnapshotInterval int64  // 快照间隔（事件数量），默认 1000
}

// NewStorageManager 创建存储管理器
func NewStorageManager(config StorageConfig) (*StorageManager, error) {
	// 设置默认值
	if config.SnapshotInterval <= 0 {
		config.SnapshotInterval = 1000
	}

	// 创建事件存储
	events, err := NewEventStore(config.DataDir)
	if err != nil {
		return nil, err
	}

	// 创建快照存储
	snapshots, err := NewSnapshotStore(config.DataDir)
	if err != nil {
		events.Close()
		return nil, err
	}

	// 创建元数据存储
	meta, err := NewMetaStore(config.DataDir)
	if err != nil {
		events.Close()
		snapshots.Close()
		return nil, err
	}

	mgr := &StorageManager{
		events:           events,
		snapshots:        snapshots,
		meta:             meta,
		snapshotInterval: config.SnapshotInterval,
	}

	return mgr, nil
}

// Events 返回事件存储
func (m *StorageManager) Events() *EventStore {
	return m.events
}

// Snapshots 返回快照存储
func (m *StorageManager) Snapshots() *SnapshotStore {
	return m.snapshots
}

// Meta 返回元数据存储
func (m *StorageManager) Meta() *MetaStore {
	return m.meta
}

// AppendEvent 追加事件
//
// 自动触发快照检查。
func (m *StorageManager) AppendEvent(event proto.GenericEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 追加事件
	if err := m.events.Append(event); err != nil {
		return err
	}

	// 检查是否需要快照
	m.eventCount++
	if m.eventCount >= m.snapshotInterval {
		m.createSnapshot()
		m.eventCount = 0
	}

	return nil
}

// AppendEventAndSave 追加事件并保存资源
func (m *StorageManager) AppendEventAndSave(event proto.GenericEvent, resource any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 追加事件（直接调用 events.Append 避免死锁）
	if err := m.events.Append(event); err != nil {
		return err
	}

	// 检查是否需要快照
	m.eventCount++
	if m.eventCount >= m.snapshotInterval {
		m.createSnapshot()
		m.eventCount = 0
	}

	// 保存资源
	if resource != nil {
		if err := m.meta.Put(event.Resource, resource); err != nil {
			return err
		}
	}

	return nil
}

// DeleteResource 删除资源
func (m *StorageManager) DeleteResource(key proto.ResourceKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 删除资源
	if err := m.meta.Delete(key); err != nil {
		return err
	}

	// 追加删除事件
	event := proto.NewGenericEvent(key, proto.EventDeleted, nil)
	if err := m.events.Append(event); err != nil {
		return err
	}

	// 检查是否需要快照
	m.eventCount++
	if m.eventCount >= m.snapshotInterval {
		m.createSnapshot()
		m.eventCount = 0
	}

	return nil
}

// createSnapshot 创建快照
func (m *StorageManager) createSnapshot() {
	// 获取当前状态
	state := m.meta.ListAll()

	// 获取当前事件序列号
	sequence := m.events.Count()

	// 保存快照
	if err := m.snapshots.Save(state, int64(sequence)); err != nil {
		// 快照失败不影响主流程
		return
	}
}

// Close 关闭所有存储
func (m *StorageManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	if err := m.events.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := m.snapshots.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := m.meta.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Stats 返回存储统计
func (m *StorageManager) Stats() StorageStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return StorageStats{
		EventCount:    m.events.Count(),
		SnapshotCount: 1, // 简化实现
		ResourceCount: m.meta.Count(""),
	}
}

// StorageStats 存储统计
type StorageStats struct {
	EventCount    int `json:"event_count"`
	SnapshotCount int `json:"snapshot_count"`
	ResourceCount int `json:"resource_count"`
}
