package http

import (
	"coinhub/docs"
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/infrastructure/security"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"
)

type apiGetHandlerSignature func(c *gin.Context, app *internal.Application) error
type apiPostHandlerSignature func(c *gin.Context, app *internal.Application) error

func apiGetHandler(_handler apiGetHandlerSignature, app *internal.Application) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, app)
	}
}

func apiPostHandler(_handler apiPostHandlerSignature, app *internal.Application) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, app)
	}
}

func SetupRouter(app *internal.Application) error {
	gin.SetMode(gin.ReleaseMode)
	gin.ForceConsoleColor()

	// TODO : setup middlewares here
	router := gin.Default()
	router.SetTrustedProxies([]string{"192.168.1.2"})

	if err := registerValidators(); err != nil {
		zap.S().Error("error in registering validators\n%s", err.Error())
		return err
	}

	docs.SwaggerInfo.BasePath = "/api/v1"
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerfiles.Handler))

	v1 := router.Group("/v1")
	if err := setupRoutes(v1, app); err != nil {
		zap.S().Error("error occurred in setting up the http GET routers\n%s", err.Error())
		return err
	}

	zap.S().Infow("HTTP server is running", "address", "localhost:8082")
	if err := router.Run(":8082"); err != nil {
		return err
	}
	return nil
}

func setupRoutes(r *gin.RouterGroup, app *internal.Application) error {
	if err := registerUserRoutes(r, app); err != nil {
		return err
	}
	if err := registerAuthRoutes(r, app); err != nil {
		return err
	}
	return nil
}

func registerUserRoutes(r *gin.RouterGroup, app *internal.Application) error {
	userGroup := r.Group("/user")
	userGroup.Use(security.AuthMiddleware())

	userGroup.GET("/ping", apiGetHandler(GetHome, app))
	return nil
}

func registerAuthRoutes(r *gin.RouterGroup, app *internal.Application) error {
	authGroup := r.Group("auth")
	authGroup.POST("/register", apiPostHandler(RegisterUser, app))
	authGroup.POST("/login/username", apiPostHandler(LoginUserWithUsername, app))
	authGroup.POST("/login/gmail", apiPostHandler(LoginUserWithGmail, app))
	return nil
}

func registerValidators() error {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterValidation("usernamecheck", schema.UsernameCheck)
		v.RegisterValidation("firstnamecheck", schema.FirstnameCheck)
		v.RegisterValidation("lastnamecheck", schema.LastnameCheck)
		v.RegisterValidation("emailcheck", schema.EmailCheck)
	}
	return nil
}
