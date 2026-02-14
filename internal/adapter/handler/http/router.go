package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/schema"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type apiGetHandlerSignature func(c *gin.Context, db *gorm.DB) error
type apiPostHandlerSignature func(c *gin.Context, db *gorm.DB) error

func apiGetHandler(_handler apiGetHandlerSignature, db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, db)
	}
}

func apiPostHandler(_handler apiPostHandlerSignature, db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, db)
	}
}

func SetupRouter(app *internal.Application) error {
	gin.SetMode(gin.ReleaseMode)
	gin.ForceConsoleColor()

	// TODO : setup middlewares
	router := gin.Default()
	router.SetTrustedProxies([]string{"192.168.1.2"})

	if err := registerValidators(); err != nil {
		zap.S().Error("error in registering validators\n%s", err.Error())
		return err
	}
	if err := setupRoutes(router, app); err != nil {
		zap.S().Error("error occurred in setting up the http GET routers\n%s", err.Error())
		return err
	}

	zap.S().Infow("HTTP server is running", "address", "localhost:8081")
	if err := router.Run(":8081"); err != nil {
		return err
	}
	return nil
}

func setupRoutes(r *gin.Engine, app *internal.Application) error {
	if err := registerUserRoutes(r, app); err != nil {
		return err
	}
	// Register other route groups...
	return nil
}

func registerUserRoutes(r *gin.Engine, app *internal.Application) error {
	userGroup := r.Group("/user")
	userGroup.GET("/test", apiGetHandler(GetHome, app.MySqlGorm))
	// userGroup.POST("/register", apiPostHandler(RegisterUser, app.MySqlGorm))
	return nil
}

func registerValidators() error {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterValidation("FirstnameCheck", schema.FirstnameCheck)
		v.RegisterValidation("LastnameCheck", schema.LastnameCheck)
		v.RegisterValidation("EmailCheck", schema.EmailCheck)
	}
	return nil
}
