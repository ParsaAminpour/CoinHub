package http

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/infrastructure/metrics"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// RequireRole allows only users whose role matches one of the given roles.
// Must be chained after AuthMiddleware (depends on "userID" being set in context).
func RequireRole(userRepo repositories.UserRepository, roles ...entities.RoleName) gin.HandlerFunc {
	return func(c *gin.Context) {
		rawID, exists := c.Get("userID")
		if !exists {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing user identity"})
			return
		}

		userID, err := uuid.Parse(rawID.(string))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid user identity"})
			return
		}

		var user entities.User
		if err := userRepo.GetUserByID(c, &user, userID); err != nil {
			zap.S().Warnw("RequireRole: user lookup failed", "userID", userID, "error", err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "user not found"})
			return
		}

		for _, allowed := range roles {
			if user.Role.Name == allowed {
				c.Next()
				return
			}
		}

		zap.S().Warnw("RequireRole: access denied", "userID", userID, "role", user.Role.Name, "required", roles)
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "insufficient permissions"})
	}
}

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
