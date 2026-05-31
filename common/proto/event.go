package proto

import (
	alog "BBgrid/common/log"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// ==================== ResourceKey ====================

// ResourceKey 资源标识 (三段式: type/namespace/name)
//
// 示例: proxy/default/my-device:8080
// 类型: client, proxy, relay, namespace
// 命名空间: default, permanent, temporary
// 名称: 具体资源标识
type ResourceKey struct {
	Type      string // 资源类型: client, proxy, relay, namespace
	Namespace string // 命名空间: default, permanent, temporary
	Name      string // 资源名称: my-device:8080
}

// String 返回完整 key
func (k ResourceKey) String() string {
	if k.Namespace == "" {
		return k.Type + "/" + k.Name
	}
	return k.Type + "/" + k.Namespace + "/" + k.Name
}

// ParseResourceKey 解析资源 key
func ParseResourceKey(s string) (ResourceKey, error) {
	parts := strings.SplitN(s, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return ResourceKey{}, fmt.Errorf("invalid resource key: %s", s)
	}
	return ResourceKey{
		Type:      parts[0],
		Namespace: parts[1],
		Name:      parts[2],
	}, nil
}

// ==================== EventType ====================

// EventType 事件类型
type EventType string

const (
	EventAdded    EventType = "ADDED"    // 资源创建
	EventModified EventType = "MODIFIED" // 资源变更
	EventDeleted  EventType = "DELETED"  // 资源删除
)

// ==================== GenericEvent ====================

// GenericEvent 通用事件结构
//
// 所有模块通过 GenericEvent 通信，不直接依赖具体业务模型。
// 支持乐观并发控制 (Version/Generation)。
type GenericEvent struct {
	// 元数据
	ID         string `json:"id"`         // 事件唯一 ID
	Sequence   int64  `json:"sequence"`   // 事件序列号 (单调递增)
	Timestamp  int64  `json:"timestamp"`  // 事件时间戳 (毫秒)
	Version    int64  `json:"version"`    // 资源版本号 (乐观并发控制)
	Generation int64  `json:"generation"` // 资源代数 (每次变更递增)

	// 资源标识
	Resource ResourceKey `json:"resource"` // 资源标识

	// 事件语义
	EventType EventType `json:"event_type"` // ADDED, MODIFIED, DELETED

	// 业务数据
	Payload any `json:"payload"` // 具体业务数据 (any 类型)
}

// NewGenericEvent 创建新的通用事件
func NewGenericEvent(resource ResourceKey, eventType EventType, payload any) GenericEvent {
	return GenericEvent{
		ID:        GenerateID(),
		Timestamp: time.Now().UnixMilli(),
		Resource:  resource,
		EventType: eventType,
		Payload:   payload,
	}
}

// WithVersion 设置版本号
func (e GenericEvent) WithVersion(version int64) GenericEvent {
	e.Version = version
	return e
}

// WithGeneration 设置代数
func (e GenericEvent) WithGeneration(generation int64) GenericEvent {
	e.Generation = generation
	return e
}

// ==================== 辅助函数 ====================

// GenerateID 生成唯一 ID
func GenerateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}

// ==================== 资源类型常量 ====================

const (
	ResourceTypeClient    = "client"
	ResourceTypeProxy     = "proxy"
	ResourceTypeRelay     = "relay"
	ResourceTypeNamespace = "namespace"
)

// ==================== 命名空间常量 ====================

const (
	NamespaceDefault   = "default"
	NamespacePermanent = "permanent"
	NamespaceTemporary = "temporary"
)
