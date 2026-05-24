package model

// NodeRole 节点角色
type NodeRole string

const (
	NodeRoleTemporary NodeRole = "temporary" // 临时节点
	NodeRolePermanent NodeRole = "permanent" // 常驻节点
)

// Namespace 命名空间
type Namespace struct {
	Name        string   `json:"name"`        // 命名空间名称
	Description string   `json:"description"` // 描述
	Type        string   `json:"type"`        // 类型: temporary, permanent, mediated
	CreatedAt   int64    `json:"created_at"`  // 创建时间
	Clients     []string `json:"clients"`     // 关联的客户端ID列表
}

// ClientNamespace 客户端命名空间信息
type ClientNamespace struct {
	ClientID  string   `json:"client_id"` // 客户端ID
	Namespace string   `json:"namespace"` // 所属命名空间
	Role      NodeRole `json:"role"`      // 节点角色
}

// PermanentNodeConfig 常驻节点配置
type PermanentNodeConfig struct {
	ClientID string `json:"client_id"` // 客户端ID
	Token    string `json:"token"`     // 认证令牌
	Secret   string `json:"secret"`    // 密钥
	Name     string `json:"name"`      // 节点名称
}
