package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type rateEntry struct {
	count    int
	lastSeen time.Time
}

func RateLimit(perMinute int) gin.HandlerFunc {
	var mu sync.Mutex
	clients := make(map[string]*rateEntry)

	go func() {
		for {
			time.Sleep(time.Minute)
			mu.Lock()
			for ip, e := range clients {
				if time.Since(e.lastSeen) > time.Minute {
					delete(clients, ip)
				} else {
					e.count = 0
				}
			}
			mu.Unlock()
		}
	}()

	return func(c *gin.Context) {
		ip := c.ClientIP()
		mu.Lock()
		e, ok := clients[ip]
		if !ok {
			e = &rateEntry{}
			clients[ip] = e
		}
		e.count++
		e.lastSeen = time.Now()
		current := e.count
		mu.Unlock()

		if current > perMinute {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code":    429,
				"message": "too many requests, try again later",
			})
			return
		}
		c.Next()
	}
}
