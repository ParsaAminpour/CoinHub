package http

import (
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func forwarded() gin.HandlerFunc {
	return func(c *gin.Context) {
		if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			c.Set("real-ip", strings.TrimSpace(parts[0]))
		}
		if xfp := c.GetHeader("X-Forwarded-Proto"); xfp != "" {
			c.Set("real-proto", xfp)
		}
		c.Next()
	}
}

func rateLimiter(maxRequests int, expiration time.Duration) gin.HandlerFunc {
	// Memory store: map[ip] -> {count, lastSeen}
	type clientInfo struct {
		count    int
		lastSeen time.Time
	}
	clients := make(map[string]*clientInfo)

	return func(c *gin.Context) {
		ip := c.GetString("real-ip")
		if ip == "" {
			ip = c.ClientIP()
		}

		now := time.Now()
		if info, ok := clients[ip]; ok {
			// Expire old
			if now.Sub(info.lastSeen) > expiration {
				info.count = 0
			}
			info.lastSeen = now
			info.count++
			if info.count > maxRequests {
				c.AbortWithStatusJSON(429, gin.H{
					"error": "too many requests",
				})
				return
			}
		} else {
			clients[ip] = &clientInfo{
				count:    1,
				lastSeen: now,
			}
		}
		c.Next()
	}
}
