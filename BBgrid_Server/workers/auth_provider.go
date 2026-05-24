package workers

// ==================== 认证接口 ====================

// Authenticator 身份验证接口
//
// 负责 Token/JWT/API Key 的验证和签发。
// 可替换为不同的实现（如 OAuth2、LDAP 等）。
type Authenticator interface {
	// ValidateClientToken 验证客户端注册令牌
	ValidateClientToken(token string) bool

	// ValidateAPIKey 验证管理 API Key
	ValidateAPIKey(apiKey string) bool

	// ValidateToken 验证 JWT Token
	ValidateToken(tokenString string) (*JWTClaims, error)

	// GenerateToken 签发 JWT Token
	GenerateToken(apiKey string) (string, error)
}

// ==================== 客户端注册表接口 ====================

// ClientRegistry 客户端注册表接口
//
// 管理客户端的注册、审批、查询。
type ClientRegistry interface {
	// GetByClientID 根据 ID 获取客户端记录
	GetByClientID(clientID string) *ClientRecord

	// GetApproved 获取所有已审批的客户端
	GetApproved() []*ClientRecord

	// GetPending 获取所有待审批的客户端
	GetPending() []*ClientRecord

	// AddApplication 添加注册申请
	AddApplication(clientID, publicKey string) error

	// Approve 审批通过
	Approve(clientID, namespace, role string) ([]byte, error)

	// Delete 删除客户端
	Delete(clientID string) bool

	// AddTempClient 添加临时客户端
	AddTempClient(clientID string) error

	// RemoveTempClient 移除临时客户端
	RemoveTempClient(clientID string)
}

// ==================== 命名空间管理接口 ====================

// NamespaceManager 命名空间管理接口
//
// 管理命名空间的 CRUD 和客户端分配。
type NamespaceManager interface {
	// ListNamespaces 列出所有命名空间
	ListNamespaces() []*NamespaceInfo

	// GetNamespace 获取指定命名空间
	GetNamespace(name string) *NamespaceInfo

	// GetClientsByNamespace 获取命名空间下的客户端
	GetClientsByNamespace(namespace string) []*ClientRecord

	// SetClientNamespace 设置客户端的命名空间
	SetClientNamespace(clientID, namespace, role string) error
}
