package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/infrastructure/security"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
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

	// docs.SwaggerInfo.BasePath = "/api/v1"
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerfiles.Handler))
	h := asynqmon.New(asynqmon.Options{
		RootPath: "/monitoring",
		RedisConnOpt: asynq.RedisClientOpt{
			Addr:     app.Configs.RedisAddress(),
			Password: app.Configs.Storage.Redis.Password,
			DB:       app.Configs.Service.QueueDB,
		},
	})

	router.GET("/monitoring", gin.WrapH(h))
	router.GET("/monitoring/*any", gin.WrapH(h))

	v1 := router.Group("/v1")
	if err := setupRoutes(v1, app); err != nil {
		zap.S().Error("error occurred in setting up the http GET routers\n%s", err.Error())
		return err
	}

	zap.S().Infow("HTTP server is running", "address", "localhost:8082")
	if err := router.Run(":8083"); err != nil {
		return err
	}
	return nil
}

func setupRoutes(r *gin.RouterGroup, app *internal.Application) error {
	r.GET("/ping", apiGetHandler(GetHome, app)) // TODO : remove this

	if err := registerSystemRoutes(r, app); err != nil {
		return err
	}
	if err := registerUserRoutes(r, app); err != nil {
		return err
	}
	if err := registerAuthRoutes(r, app); err != nil {
		return err
	}
	if err := registerTransactionRoutes(r, app); err != nil {
		return err
	}
	if err := registerOrderRoutes(r, app); err != nil {
		return err
	}
	return nil
}

// TODO : don't pass the entire application structure to the HTTP handlers!
func registerOrderRoutes(r *gin.RouterGroup, app *internal.Application) error {
	orderGroup := r.Group("/order")
	orderGroup.Use(security.AuthMiddleware())
	orderGroup.POST("/limit", apiPostHandler(PlaceLimitOrderHTTPHandler, app))
	orderGroup.POST("/market", apiPostHandler(PlaceMarketOrderHTTPHandler, app))
	orderGroup.DELETE("/cancel", apiPostHandler(CancelOrderHTTPHandler, app))
	return nil
}

func registerUserRoutes(r *gin.RouterGroup, app *internal.Application) error {
	userGroup := r.Group("/user")
	userGroup.Use(security.AuthMiddleware())
	return nil
}

func registerAuthRoutes(r *gin.RouterGroup, app *internal.Application) error {
	authGroup := r.Group("auth")
	authGroup.POST("/register", apiPostHandler(RegisterUserHandler, app))
	authGroup.POST("/login/username", apiPostHandler(LoginUserWithUsernameHandler, app))
	authGroup.POST("/login/gmail", apiPostHandler(LoginUserWithGmailHandler, app))
	authGroup.POST("/verify/gmail-code", apiPostHandler(VerifyGmailVerificationCode, app))
	authGroup.POST("/resend/gmail-code", apiPostHandler(ResendGmailVerificationCodeHandler, app))
	authGroup.POST("/mock/login", apiPostHandler(func(c *gin.Context, app *internal.Application) error {
		responseHelper := helper.NewResponseHelper()
		jwtToken, err := security.GenerateToken("parsa") // TODO : remove this
		if err != nil {
			return err
		}
		responseHelper.SuccessStandard(c, schema.LoginUserResponse{
			Code:     http.StatusOK,
			Message:  "user logged in with username successfully",
			JWTToken: jwtToken,
		})
		return nil
	}, app))
	return nil
}

func registerTransactionRoutes(r *gin.RouterGroup, app *internal.Application) error {
	authGroup := r.Group("transaction")
	authGroup.Use(security.AuthMiddleware())
	authGroup.POST("/withdraw", apiPostHandler(WithdrawHandler, app))
	return nil
}

// NOTE : Contains system and sensitive operation which means it should be highly protected and the access control applied.
// TODO : implement the routing strategy of this section after the access control added.
func registerSystemRoutes(r *gin.RouterGroup, app *internal.Application) error {
	systemGroup := r.Group("system")
	systemGroup.Use(security.AuthMiddleware())

	operationGroup := systemGroup.Group("operation")
	assetOperationsGroup := operationGroup.Group("asset")
	assetOperationsGroup.POST("/add", apiPostHandler(CreateAssetAdminOperationHandler, app))
	return nil
}

func registerValidators() error {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterValidation("usernamecheck", schema.UsernameCheck)
		v.RegisterValidation("firstnamecheck", schema.FirstnameCheck)
		v.RegisterValidation("lastnamecheck", schema.LastnameCheck)
		v.RegisterValidation("emailcheck", schema.EmailCheck)
		v.RegisterValidation("walletaddresscheck", schema.WalletAddressCheck)
		v.RegisterValidation("decimal_gt0", schema.DecimalGT0Check)
		v.RegisterValidation("client_ord_id", schema.ClientOrdIDCheck)
		v.RegisterValidation("symbol_format", schema.SymbolFormatCheck)
		v.RegisterValidation("future_time", schema.FutureTimeCheck)
		v.RegisterStructValidation(schema.ValidatePlaceOrderRequest, schema.PlaceOrderRequest{})
		v.RegisterStructValidation(schema.ValidateCancelOrderRequest, schema.CancelOrderRequest{})
	}
	return nil
}
