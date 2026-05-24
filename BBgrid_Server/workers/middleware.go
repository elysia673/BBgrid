package workers

import (
	"net/http"
	"strings"
	"sync"
	"time"

	alog "BBgrid/common/log"
)

// JWTAuth JWT 认证中间件
//
// 验证请求头中的 JWT Token。
// 格式：Authorization: Bearer <token>
func JWTAuth(auth *AuthWorker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 获取 Authorization 头
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, `{"code":401,"msg":"missing authorization header"}`, http.StatusUnauthorized)
				return
			}

			// 检查格式
			if !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, `{"code":401,"msg":"invalid authorization format"}`, http.StatusUnauthorized)
				return
			}

			// 提取 token
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" {
				http.Error(w, `{"code":401,"msg":"empty token"}`, http.StatusUnauthorized)
				return
			}

			// 验证 token
			claims, err := auth.ValidateToken(token)
			if err != nil {
				http.Error(w, `{"code":401,"msg":"invalid token"}`, http.StatusUnauthorized)
				return
			}

			// 将 claims 存入请求上下文
			r.Header.Set("X-API-Key", claims.APIKey)

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit 限流中间件
//
// 基于 IP 的令牌桶限流。
func RateLimit(perMinute int) func(http.Handler) http.Handler {
	if perMinute <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	type bucket struct {
		tokens    float64
		lastTime  time.Time
	}
	var (
		mu      sync.Mutex
		buckets = make(map[string]*bucket)
		rate    = float64(perMinute) / 60.0
		maxTok  = float64(perMinute)
	)

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			mu.Lock()
			for ip, b := range buckets {
				if time.Since(b.lastTime) > 10*time.Minute {
					delete(buckets, ip)
				}
			}
			mu.Unlock()
		}
	}()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
				ip = strings.Split(fwd, ",")[0]
			}

			mu.Lock()
			b, exists := buckets[ip]
			if !exists {
				b = &bucket{tokens: maxTok, lastTime: time.Now()}
				buckets[ip] = b
			}
			elapsed := time.Since(b.lastTime).Seconds()
			b.tokens += elapsed * rate
			if b.tokens > maxTok {
				b.tokens = maxTok
			}
			b.lastTime = time.Now()

			if b.tokens < 1 {
				mu.Unlock()
				alog.Warn(alog.CatServer, "rate limit exceeded", "ip", ip)
				http.Error(w, `{"code":429,"msg":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}
			b.tokens--
			mu.Unlock()

			next.ServeHTTP(w, r)
		})
	}
}
