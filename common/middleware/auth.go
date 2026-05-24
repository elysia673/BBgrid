// Package middleware 提供 HTTP 中间件
package middleware

import (
	"BBgrid/BBgrid_Server/workers"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AuthMiddleware 认证中间件
//
// 将认证逻辑从 main.go 中独立出来，
// 支持单用户模式（API Key + JWT Token）。
type AuthMiddleware struct {
	auth *workers.AuthWorker
}

// NewAuthMiddleware 创建认证中间件
func NewAuthMiddleware(auth *workers.AuthWorker) *AuthMiddleware {
	return &AuthMiddleware{auth: auth}
}

// RequireJWT 要求 JWT Token 认证
func (m *AuthMiddleware) RequireJWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取 Authorization 头
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "missing authorization header"})
			c.Abort()
			return
		}

		// 检查格式
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid authorization format"})
			c.Abort()
			return
		}

		// 提取 token
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "empty token"})
			c.Abort()
			return
		}

		// 验证 token
		claims, err := m.auth.ValidateToken(token)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid token"})
			c.Abort()
			return
		}

		// 将 API Key 存入上下文
		c.Set("api_key", claims.APIKey)
		c.Next()
	}
}

// RequireAPIKey 要求 API Key 认证（用于登录等端点）
func (m *AuthMiddleware) RequireAPIKey() gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		if apiKey == "" {
			// 尝试从 query 参数获取
			apiKey = c.Query("api_key")
		}

		if apiKey == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "missing API key"})
			c.Abort()
			return
		}

		if !m.auth.ValidateAPIKey(apiKey) {
			c.JSON(http.StatusUnauthorized, gin.H{"code": 401, "msg": "invalid API key"})
			c.Abort()
			return
		}

		c.Set("api_key", apiKey)
		c.Next()
	}
}

// OptionalJWT 可选 JWT Token 认证
//
// 如果提供了 Token 则验证，否则跳过。
func (m *AuthMiddleware) OptionalJWT() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.Next()
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.Next()
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		if token == "" {
			c.Next()
			return
		}

		claims, err := m.auth.ValidateToken(token)
		if err != nil {
			c.Next()
			return
		}

		c.Set("api_key", claims.APIKey)
		c.Next()
	}
}
