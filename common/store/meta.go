package store

import (
	"BBgrid/common/proto"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ==================== 元数据存储 ====================

// MetaStore 元数据存储
//
// 资源 CRUD 操作。
// 每个资源类型一个文件，JSON 格式。
type MetaStore struct {
	mu        sync.RWMutex
	dir       string
	resources map[string]map[string]any // type -> name -> resource
}

// NewMetaStore 创建元数据存储
func NewMetaStore(dataDir string) (*MetaStore, error) {
	dir := filepath.Join(dataDir, "meta")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create meta store dir: %w", err)
	}

	store := &MetaStore{
		dir:       dir,
		resources: make(map[string]map[string]any),
	}

	// 加载已有数据
	if err := store.loadAll(); err != nil {
		return nil, fmt.Errorf("load meta data: %w", err)
	}

	return store, nil
}

// Put 保存资源
func (s *MetaStore) Put(key proto.ResourceKey, resource any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 确保资源类型存在
	if s.resources[key.Type] == nil {
		s.resources[key.Type] = make(map[string]any)
	}

	// 保存到内存
	name := key.String()
	s.resources[key.Type][name] = resource

	// 保存到文件
	return s.saveType(key.Type)
}

// Get 获取资源
func (s *MetaStore) Get(key proto.ResourceKey) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.resources[key.Type] == nil {
		return nil, false
	}

	name := key.String()
	resource, ok := s.resources[key.Type][name]
	return resource, ok
}

// Delete 删除资源
func (s *MetaStore) Delete(key proto.ResourceKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.resources[key.Type] == nil {
		return nil
	}

	name := key.String()
	delete(s.resources[key.Type], name)

	// 保存到文件
	return s.saveType(key.Type)
}

// List 列出指定类型的所有资源
func (s *MetaStore) List(resourceType string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.resources[resourceType] == nil {
		return nil
	}

	// 复制一份
	result := make(map[string]any)
	for k, v := range s.resources[resourceType] {
		result[k] = v
	}
	return result
}

// ListAll 列出所有资源
func (s *MetaStore) ListAll() map[string]map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 复制一份
	result := make(map[string]map[string]any)
	for typeName, resources := range s.resources {
		result[typeName] = make(map[string]any)
		for k, v := range resources {
			result[typeName][k] = v
		}
	}
	return result
}

// Count 返回指定类型的资源数量
func (s *MetaStore) Count(resourceType string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.resources[resourceType] == nil {
		return 0
	}
	return len(s.resources[resourceType])
}

// Close 关闭存储
func (s *MetaStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 保存所有类型
	for typeName := range s.resources {
		if err := s.saveType(typeName); err != nil {
			return err
		}
	}
	return nil
}

// loadAll 加载所有资源
func (s *MetaStore) loadAll() error {
	pattern := filepath.Join(s.dir, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	for _, file := range files {
		// 从文件名提取资源类型
		typeName := filepath.Base(file)
		typeName = typeName[:len(typeName)-5] // 去掉 .json

		// 读取文件
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		// 解析 JSON
		var resources map[string]any
		if err := json.Unmarshal(data, &resources); err != nil {
			continue
		}

		s.resources[typeName] = resources
	}

	return nil
}

// saveType 保存指定类型的资源到文件
func (s *MetaStore) saveType(typeName string) error {
	filename := filepath.Join(s.dir, typeName+".json")
	tmpFilename := filename + ".tmp"

	data, err := json.MarshalIndent(s.resources[typeName], "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", typeName, err)
	}

	// 原子写入：先写临时文件，再 rename
	if err := os.WriteFile(tmpFilename, data, 0644); err != nil {
		return fmt.Errorf("write %s tmp: %w", typeName, err)
	}

	// 同步到磁盘
	f, err := os.Open(tmpFilename)
	if err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("open %s tmp: %w", typeName, err)
	}
	f.Sync()
	f.Close()

	// rename 是原子操作
	if err := os.Rename(tmpFilename, filename); err != nil {
		os.Remove(tmpFilename)
		return fmt.Errorf("rename %s: %w", typeName, err)
	}

	return nil
}
