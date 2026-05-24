package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ==================== 快照存储 ====================

// SnapshotStore 快照存储
//
// 定期保存状态快照，用于快速恢复。
// 避免从头回放所有事件。
type SnapshotStore struct {
	mu     sync.Mutex
	dir    string
	latest *Snapshot
}

// Snapshot 状态快照
type Snapshot struct {
	ID        string                    `json:"id"`
	Timestamp int64                     `json:"timestamp"`
	Sequence  int64                     `json:"sequence"` // 快照对应的事件序列号
	State     map[string]map[string]any `json:"state"`    // 资源状态 (type -> name -> resource)
}

// NewSnapshotStore 创建快照存储
func NewSnapshotStore(dataDir string) (*SnapshotStore, error) {
	dir := filepath.Join(dataDir, "snapshots")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create snapshot store dir: %w", err)
	}

	store := &SnapshotStore{
		dir: dir,
	}

	// 加载最新快照
	if err := store.loadLatest(); err != nil {
		return nil, fmt.Errorf("load latest snapshot: %w", err)
	}

	return store, nil
}

// Save 保存快照
func (s *SnapshotStore) Save(state map[string]map[string]any, sequence int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := &Snapshot{
		ID:        generateID(),
		Timestamp: time.Now().UnixMilli(),
		Sequence:  sequence,
		State:     state,
	}

	// 序列化
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	// 原子写入：先写临时文件，再 rename
	filename := filepath.Join(s.dir, fmt.Sprintf("%d.json", snapshot.Timestamp))
	tmpFilename := filename + ".tmp"

	if err := os.WriteFile(tmpFilename, data, 0644); err != nil {
		return fmt.Errorf("write snapshot tmp: %w", err)
	}

	// 同步到磁盘
	f, err := os.Open(tmpFilename)
	if err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("open snapshot tmp: %w", err)
	}
	f.Sync()
	f.Close()

	// rename 是原子操作
	if err := os.Rename(tmpFilename, filename); err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("rename snapshot: %w", err)
	}

	// 更新最新快照
	s.latest = snapshot

	// 清理旧快照（保留最近 5 个）
	s.cleanupOldSnapshots(5)

	return nil
}

// Load 加载最新快照
func (s *SnapshotStore) Load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.latest == nil {
		return nil, fmt.Errorf("no snapshot available")
	}
	return s.latest, nil
}

// HasSnapshot 检查是否有快照
func (s *SnapshotStore) HasSnapshot() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest != nil
}

// GetSequence 获取快照对应的事件序列号
func (s *SnapshotStore) GetSequence() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		return 0
	}
	return s.latest.Sequence
}

// Close 关闭存储
func (s *SnapshotStore) Close() error {
	return nil
}

// loadLatest 加载最新快照
func (s *SnapshotStore) loadLatest() error {
	pattern := filepath.Join(s.dir, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return nil
	}

	// 找到最新的文件
	var latestFile string
	var latestTime int64
	for _, file := range files {
		var timestamp int64
		fmt.Sscanf(filepath.Base(file), "%d.json", &timestamp)
		if timestamp > latestTime {
			latestTime = timestamp
			latestFile = file
		}
	}

	if latestFile == "" {
		return nil
	}

	// 读取快照
	data, err := os.ReadFile(latestFile)
	if err != nil {
		return err
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	s.latest = &snapshot
	return nil
}

// cleanupOldSnapshots 清理旧快照
func (s *SnapshotStore) cleanupOldSnapshots(keep int) {
	pattern := filepath.Join(s.dir, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	if len(files) <= keep {
		return
	}

	// 按时间排序
	sortFiles(files)

	// 删除旧文件
	for i := 0; i < len(files)-keep; i++ {
		os.Remove(files[i])
	}
}

// sortFiles 按文件名排序（时间戳）
func sortFiles(files []string) {
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[i] > files[j] {
				files[i], files[j] = files[j], files[i]
			}
		}
	}
}

// generateID 生成唯一 ID
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
