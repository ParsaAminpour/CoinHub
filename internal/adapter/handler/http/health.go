package http

import (
	"context"
	"coinhub/internal"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type serviceStatus struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type healthResponse struct {
	Status   string                   `json:"status"`
	Services map[string]serviceStatus `json:"services"`
}

func HealthCheckHandler(c *gin.Context, app *internal.Application) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	services := make(map[string]serviceStatus)
	overall := "ok"

	check := func(name string, err error) {
		if err != nil {
			services[name] = serviceStatus{Status: "error", Error: err.Error()}
			overall = "degraded"
		} else {
			services[name] = serviceStatus{Status: "ok"}
		}
	}

	// PostgreSQL
	if app.MySqlGorm != nil {
		sqlDB, err := app.MySqlGorm.DB()
		if err == nil {
			err = sqlDB.PingContext(ctx)
		}
		check("postgres", err)
	}

	// Redis
	if app.RedisClient != nil {
		check("redis", app.RedisClient.Ping(ctx).Err())
	}

	// Kafka
	if app.EngineEventProducer != nil {
		check("kafka", app.EngineEventProducer.Ping(ctx))
	}

	statusCode := http.StatusOK
	if overall == "degraded" {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, healthResponse{
		Status:   overall,
		Services: services,
	})
}
