package middleware

import (
	alog "BBgrid/common/log"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

var (
	jwtSecret     []byte
	jwtSecretFile string
	registerDir   string
)

// Init 初始化中间件数据目录
func Init(dataDir string) {
	jwtSecretFile = filepath.Join(dataDir, "jwt_secret")
	registerDir = filepath.Join(dataDir, "registered_keys")
}

func InitJWTSecret() {
	// 尝试从文件加载
	data, err := os.ReadFile(jwtSecretFile)
	if err == nil && len(data) >= 32 {
		jwtSecret = data[:32]
		alog.Info(alog.CatConfig, "JWT密钥已从文件加载")
		return
	}

	// 生成新密钥并保存
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		alog.Fatal(alog.CatConfig, "生成JWT密钥失败", "error", err)
	}
	jwtSecret = b

	os.MkdirAll(filepath.Dir(jwtSecretFile), 0755)
	if err := os.WriteFile(jwtSecretFile, b, 0600); err != nil {
		alog.Error(alog.CatConfig, "保存JWT密钥失败", "error", err)
	}
	alog.Info(alog.CatConfig, "JWT密钥已随机生成并保存")
}

type Claims struct {
	APIKey string `json:"api_key"`
	jwt.RegisteredClaims
}

func getKeyFilePath(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	filename := hex.EncodeToString(hash[:])
	return filepath.Join(registerDir, filename)
}

func isKeyRegistered(apiKey string) bool {
	path := getKeyFilePath(apiKey)
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func markKeyRegistered(apiKey string) error {
	err := os.MkdirAll(registerDir, 0755)
	if err != nil {
		alog.Error(alog.CatAuth, "创建目录失败", "error", err, "path", registerDir)
		return err
	}
	path := getKeyFilePath(apiKey)
	alog.Info(alog.CatAuth, "创建占位文件", "path", path)
	err = os.WriteFile(path, []byte("registered"), 0644)
	if err != nil {
		alog.Error(alog.CatAuth, "写入文件失败", "error", err, "path", path)
		return err
	}
	return nil
}

func CleanupExpiredRegistrations() {
	os.MkdirAll(registerDir, 0755)

	files, err := os.ReadDir(registerDir)
	if err != nil {
		return
	}

	for _, file := range files {
		path := filepath.Join(registerDir, file.Name())
		info, err := file.Info()
		if err != nil {
			continue
		}

		// 删除超过 1 年的注册文件
		if time.Since(info.ModTime()) > 365*24*time.Hour {
			os.Remove(path)
		}
	}
}

// GenerateToken 生成 JWT Token
func GenerateToken(apiKey string) (string, error) {
	claims := Claims{
		APIKey: apiKey,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(365 * 24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", fmt.Errorf("生成 Token 失败: %w", err)
	}

	// 标记为已注册（用于撤销检查）
	if err := markKeyRegistered(apiKey); err != nil {
		alog.Error(alog.CatAuth, "记录API Key失败", "error", err)
	}

	return tokenStr, nil
}

func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "missing authorization header"})
			return
		}

		tokenString := authHeader[7:]
		claims := &Claims{}

		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtSecret, nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		// 实时检查注册文件是否还存在
		if !isKeyRegistered(claims.APIKey) {
			c.AbortWithStatusJSON(401, gin.H{"error": "token revoked, please login again"})
			return
		}

		c.Set("api_key", claims.APIKey)
		c.Next()
	}
}
