package http

import (
	"coinhub/internal"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type apiGetHandlerSignature func(c *gin.Context, db *gorm.DB) error

func apiGetHandler(_handler apiGetHandlerSignature, db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, db)
	}
}

func SetupRouter(app *internal.Application) error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.SetTrustedProxies([]string{"192.168.1.2"})

	router.GET("/test", apiGetHandler(GetHome, app.MySqlGorm))

	zap.S().Infow("HTTP server is running", "address", "localhost:8081")
	if err := router.Run(":8081"); err != nil {
		return err
	}
	return nil
}
