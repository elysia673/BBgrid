// Package store 提供持久化存储实现
//
// 包含三种存储：
// - EventStore: 事件流存储（追加写入，不可变）
// - SnapshotStore: 快照存储（定期保存状态快照）
// - MetaStore: 元数据存储（资源 CRUD）
package store

import (
	"BBgrid/common/proto"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ==================== 事件存储 ====================

// EventStore 事件流存储
//
// 追加写入，不可变日志。
// 用于审计、回放、调试。
type EventStore struct {
	mu       sync.Mutex
	dir      string
	events   []proto.GenericEvent
	file     *os.File
	sequence int64
}

// NewEventStore 创建事件存储
func NewEventStore(dataDir string) (*EventStore, error) {
	dir := filepath.Join(dataDir, "events")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create event store dir: %w", err)
	}

	store := &EventStore{
		dir: dir,
	}

	// 加载已有事件
	if err := store.loadEvents(); err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	// 打开当前日志文件
	if err := store.openFile(); err != nil {
		return nil, fmt.Errorf("open event file: %w", err)
	}

	return store, nil
}

// Append 追加事件
func (s *EventStore) Append(event proto.GenericEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 设置序列号
	s.sequence++
	event.Sequence = s.sequence

	// 设置时间戳
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}

	// 序列化
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	// 追加到文件
	data = append(data, '\n')
	if _, err := s.file.Write(data); err != nil {
		return fmt.Errorf("write event: %w", err)
	}

	// 同步到磁盘
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync event: %w", err)
	}

	// 保存到内存
	s.events = append(s.events, event)

	return nil
}

// Query 查询事件
func (s *EventStore) Query(filter EventFilter) []proto.GenericEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []proto.GenericEvent
	for _, event := range s.events {
		if filter.Matches(event) {
			result = append(result, event)
		}
	}
	return result
}

// Count 返回事件数量
func (s *EventStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// Close 关闭存储
func (s *EventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// loadEvents 加载已有事件
func (s *EventStore) loadEvents() error {
	pattern := filepath.Join(s.dir, "*.jsonl")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		lines := splitLines(data)
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			var event proto.GenericEvent
			if err := json.Unmarshal(line, &event); err != nil {
				continue
			}
			s.events = append(s.events, event)
			if event.Sequence > s.sequence {
				s.sequence = event.Sequence
			}
		}
	}

	return nil
}

// openFile 打开当前日志文件
func (s *EventStore) openFile() error {
	filename := filepath.Join(s.dir, fmt.Sprintf("%d.jsonl", time.Now().Unix()))
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	s.file = f
	return nil
}

// EventFilter 事件过滤器
type EventFilter struct {
	ResourceType string
	Namespace    string
	EventType    proto.EventType
	StartTime    int64
	EndTime      int64
}

// Matches 检查事件是否匹配过滤器
func (f EventFilter) Matches(event proto.GenericEvent) bool {
	if f.ResourceType != "" && event.Resource.Type != f.ResourceType {
		return false
	}
	if f.Namespace != "" && event.Resource.Namespace != f.Namespace {
		return false
	}
	if f.EventType != "" && event.EventType != f.EventType {
		return false
	}
	if f.StartTime > 0 && event.Timestamp < f.StartTime {
		return false
	}
	if f.EndTime > 0 && event.Timestamp > f.EndTime {
		return false
	}
	return true
}

// splitLines 按行分割
func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}
