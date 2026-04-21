package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// HTTPMiddleware records per-request Prometheus metrics (count + latency).
// Use c.FullPath() so dynamic segments like /order/:id are grouped, not split
// into per-ID cardinality explosions.
func HTTPMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		path := c.FullPath()
		if path == "" {
			// Unmatched routes (404s) — group them to avoid unbounded cardinality.
			path = "unmatched"
		}

		status := strconv.Itoa(c.Writer.Status())
		duration := time.Since(start).Seconds()

		HTTPRequestsTotal.WithLabelValues(c.Request.Method, path, status).Inc()
		HTTPRequestDuration.WithLabelValues(c.Request.Method, path).Observe(duration)
	}
}
