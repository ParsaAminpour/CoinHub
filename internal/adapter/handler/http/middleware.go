package http

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/infrastructure/metrics"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
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

func rateLimiter(store *cache.RateLimiterCache, maxRequests int) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.GetString("real-ip")
		if ip == "" {
			ip = c.ClientIP()
		}

		allowed, err := store.Allow(c.Request.Context(), ip, maxRequests)
		if err != nil {
			// Redis failure: log and let the request through to avoid blocking legitimate traffic.
			zap.S().Warnw("rate limiter redis error, allowing request", "ip", ip, "error", err)
			c.Next()
			return
		}

		if !allowed {
			metrics.RateLimitedRequestsTotal.WithLabelValues(ip).Inc()
			c.Header("Retry-After", time.Now().Add(time.Minute).UTC().Format(http.TimeFormat))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			return
		}

		c.Next()
	}
}
