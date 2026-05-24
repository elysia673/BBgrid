// Package storage 提供代理配置持久化
//
// 使用 JSON 文件存储代理配置，支持自动加载和保存。
package storage

import (
	alog "BBgrid/common/log"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ProxyRecord 代理记录
type ProxyRecord struct {
	ClientID   string `json:"client_id"`
	RemotePort int    `json:"remote_port"`
	LocalPort  int    `json:"local_port"`
	LocalIP    string `json:"local_ip"`
	Protocol   string `json:"protocol"`
	BindAddr   string `json:"bind_addr"`
}

// Storage 持久化存储
type Storage struct {
	path    string
	mu      sync.RWMutex
	proxies map[string]ProxyRecord // key: "clientID-remotePort"
}

// New 创建存储实例
//
// path 为 JSON 文件路径，不存在则创建。
//
//	&Storage{} 创建结构体指针
func New(path string) (*Storage, error) {
	s := &Storage{
		path:    path,
		proxies: make(map[string]ProxyRecord),
	}

	// 确保目录存在
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("创建目录: %w", err)
	}

	// 加载已有数据
	if err := s.load(); err != nil {
		alog.Info(alog.CatConfig, "加载存储文件（将创建新文件）", "error", err)
	}

	return s, nil
}

// load 从文件加载数据
func (s *Storage) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}

	var proxies []ProxyRecord
	// 用于 JSON 反序列化 json -> go
	if err := json.Unmarshal(data, &proxies); err != nil {
		return fmt.Errorf("解析存储文件: %w", err)
	}

	s.proxies = make(map[string]ProxyRecord)
	// 循环写入
	for _, p := range proxies {
		key := s.makeKey(p.ClientID, p.RemotePort)
		s.proxies[key] = p
	}

	alog.Info(alog.CatConfig, "已加载代理配置", "count", len(s.proxies))
	return nil
}

// save 保存数据到文件
func (s *Storage) save() error {
	proxies := make([]ProxyRecord, 0, len(s.proxies))
	for _, p := range s.proxies {
		proxies = append(proxies, p)
	}

	data, err := json.MarshalIndent(proxies, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化数据: %w", err)
	}

	if err := os.WriteFile(s.path, data, 0644); err != nil {
		return fmt.Errorf("写入文件: %w", err)
	}

	return nil
}

// makeKey 生成代理键
func (s *Storage) makeKey(clientID string, remotePort int) string {
	return fmt.Sprintf("%s-%d", clientID, remotePort)
}

// Add 添加代理记录
func (s *Storage) Add(record ProxyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(record.ClientID, record.RemotePort)
	s.proxies[key] = record

	return s.save()
}

// Remove 删除代理记录
func (s *Storage) Remove(clientID string, remotePort int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := s.makeKey(clientID, remotePort)
	delete(s.proxies, key)

	return s.save()
}

// RemoveByClient 删除客户端所有代理
func (s *Storage) RemoveByClient(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	changed := false
	for key, p := range s.proxies {
		if p.ClientID == clientID {
			delete(s.proxies, key)
			changed = true
		}
	}

	if changed {
		return s.save()
	}
	return nil
}

// GetAll 获取所有代理记录
func (s *Storage) GetAll() []ProxyRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	proxies := make([]ProxyRecord, 0, len(s.proxies))
	for _, p := range s.proxies {
		proxies = append(proxies, p)
	}
	return proxies
}

// GetByClient 获取客户端所有代理
func (s *Storage) GetByClient(clientID string) []ProxyRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var proxies []ProxyRecord
	for _, p := range s.proxies {
		if p.ClientID == clientID {
			proxies = append(proxies, p)
		}
	}
	return proxies
}
