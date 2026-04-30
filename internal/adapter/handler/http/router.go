package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/schema"
	coinhub_ws "coinhub/internal/adapter/handler/websockets"
	"coinhub/internal/adapter/handler/websockets/notification"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/infrastructure/metrics"
	"coinhub/internal/infrastructure/security"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	swaggerfiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"
	"nhooyr.io/websocket"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpAPIHandler interface{}

type apiGetHandlerSignature func(c *gin.Context, handlerCtx *HttpAPIHandler) error
type apiPostHandlerSignature func(c *gin.Context, handlerCtx *HttpAPIHandler) error
type apiWebsocketHandlerSignature func(c *gin.Context, client *coinhub_ws.Client) error

func apiGetHandler(_handler apiGetHandlerSignature, handlerCtx *HttpAPIHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, handlerCtx)
	}
}

func apiPostHandler(_handler apiPostHandlerSignature, handlerCtx *HttpAPIHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		_handler(c, handlerCtx)
	}
}

func apiWebsocketHandler(_handlerReadLoop apiWebsocketHandlerSignature, userRepository repositories.UserRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, exist := c.Get("userID")
		if !exist {
			zap.S().Infow("userID missing in context")
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		// Pass the raw http.ResponseWriter to websocket.Accept, bypassing gin's wrapper.
		// nhooyr.io/websocket calls WriteHeaderNow() on gin's writer before Hijack(),
		// which marks the response as written and causes gin's own Hijack() to fail.
		// Unwrap() is on the concrete *responseWriter struct, not the interface, so we
		// reach it via a local interface assertion.
		type unwrappable interface{ Unwrap() http.ResponseWriter }
		rawWriter, ok := c.Writer.(unwrappable)
		if !ok {
			zap.S().Errorw("response writer is not unwrappable")
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		conn, err := websocket.Accept(rawWriter.Unwrap(), c.Request, &websocket.AcceptOptions{
			InsecureSkipVerify: true, // TODO : lock this down to actual origin in prod.
		})
		if err != nil {
			zap.S().Errorw("failed to accept websocket connection", "error", err)
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(notification.MAX_MSG_BYTES)

		parsedUserID, err := uuid.Parse(userID.(string))
		if err != nil {
			zap.S().Errorw("invalid userID in token", "error", err)
			conn.Close(websocket.StatusInternalError, "invalid user ID")
			return
		}

		var user entities.User
		if err := userRepository.GetUserByID(c, &user, parsedUserID); err != nil {
			zap.S().Errorw("failed to get user for websocket", "error", err)
			conn.Close(websocket.StatusInternalError, "user lookup failed")
			return
		}

		client := coinhub_ws.NewClient(user.ID.String(), user.ID.String(), conn)
		_handlerReadLoop(c, client)
	}
}

func SetupRouter(app *internal.Application) error {
	gin.SetMode(gin.ReleaseMode)
	gin.ForceConsoleColor()

	router := gin.Default()
	// TODO(security) : add Sentry for crash reporting - in production
	router.Use(gin.Recovery())           // for handling panics
	router.Use(gin.Logger())             // write the logs to gin.DefaultWriter
	router.Use(metrics.HTTPMiddleware()) // prometheus HTTP metrics

	if app.Configs.App.Env != "DEVELOPMENT" {
		router.Use(security.SecurityHeadersMiddleware())  // security response headers
		router.Use(forwarded())                           // extract real IP from X-Forwarded-For
		router.Use(rateLimiter(app.RateLimiterCache, 60)) // 60 req/min per IP
		// router.SetTrustedProxies([]string{"192.168.1.2"}) // TODO(security) : add the load balancer address here
		router.Use(cors.New(cors.Config{
			AllowOrigins:     []string{app.Configs.AllowedOrigins.FrontendApplication},
			AllowMethods:     []string{"GET", "POST", "PUT", "DELETE"},
			AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
			ExposeHeaders:    []string{"Content-Length"},
			AllowCredentials: true,
			MaxAge:           12 * time.Hour,
		}))
	}

	if err := registerValidators(); err != nil {
		zap.S().Error("error in registering validators\n%s", err.Error())
		return err
	}

	// Prometheus metrics — scraped by Prometheus server, not for public clients.
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

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
	r.GET("/health", func(c *gin.Context) { HealthCheckHandler(c, app) })

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

func registerOrderRoutes(r *gin.RouterGroup, app *internal.Application) error {
	orderHandler := NewOrderHandler(
		app.UserRepository,
		app.TxManager,
		app.OrderMatchEngine,
		app.EngineEventProducer,
		app.MarketPriceFeed,
	)

	orderGroup := r.Group("/order")
	orderGroup.Use(security.AuthMiddleware())
	orderGroup.Use(RequireRole(app.UserRepository, entities.RoleUser))

	orderGroup.POST("/limit", apiPostHandler(PlaceLimitOrderHTTPHandler, &orderHandler))
	orderGroup.POST("/market", apiPostHandler(PlaceMarketOrderHTTPHandler, &orderHandler))
	orderGroup.DELETE("/cancel", apiPostHandler(CancelOrderHTTPHandler, &orderHandler))

	if err := registerOrderWebsocketRoutes(orderGroup, app); err != nil {
		return err
	}
	return nil
}

func registerOrderWebsocketRoutes(r *gin.RouterGroup, app *internal.Application) error {
	eventsGroup := r.Group("/events")
	eventsGroup.Use(RequireRole(app.UserRepository, entities.RoleUser))
	eventsGroup.GET("/ws", apiWebsocketHandler(app.WebsocketNotificationServer.OrderEventEmitterWebsocketListener, app.UserRepository))
	return nil
}

func registerUserRoutes(r *gin.RouterGroup, app *internal.Application) error {
	userGroup := r.Group("/user")
	userGroup.Use(security.AuthMiddleware())
	userGroup.Use(RequireRole(app.UserRepository, entities.RoleUser))
	return nil
}

func registerAuthRoutes(r *gin.RouterGroup, app *internal.Application) error {
	authHandler := NewAuthHandler(
		&app.TxManager,
		&app.WalletService,
		app.UserRepository,
		app.RedisClient,
		app.AuthGmailCache,
		app.AsynqClient,
		*app.Configs,
	)

	authGroup := r.Group("auth")
	authGroup.POST("/register", apiPostHandler(RegisterUserHandler, &authHandler))
	authGroup.POST("/login/username", apiPostHandler(LoginUserWithUsernameHandler, &authHandler))
	authGroup.POST("/login/gmail", apiPostHandler(LoginUserWithGmailHandler, &authHandler))
	authGroup.POST("/verify/gmail-code", apiPostHandler(VerifyGmailVerificationCode, &authHandler))
	authGroup.POST("/resend/gmail-code", apiPostHandler(ResendGmailVerificationCodeHandler, &authHandler))
	return nil
}

func registerTransactionRoutes(r *gin.RouterGroup, app *internal.Application) error {
	transactionHandler := NewTransactionHandler(
		app.UserRepository,
		app.WalletAccountRepository,
		app.TransactinRepository,
		app.WalletService,
		app.ETHClient,
		app.AsynqClient,
		app.PendingTransactionsCache,
	)

	transactionGroup := r.Group("transaction")
	transactionGroup.Use(security.AuthMiddleware())
	transactionGroup.Use(RequireRole(app.UserRepository, entities.RoleUser))
	transactionGroup.POST("/withdraw", apiPostHandler(WithdrawHandler, &transactionHandler))
	return nil
}

// NOTE : Contains system and sensitive operations — requires admin or system role.
func registerSystemRoutes(r *gin.RouterGroup, app *internal.Application) error {
	systemHandler := NewSystemHandler(
		app.TradingPairRepository,
		app.KafkaTopicManager,
	)

	systemGroup := r.Group("system")
	systemGroup.Use(security.AuthMiddleware())
	systemGroup.Use(RequireRole(app.UserRepository, entities.RoleAdmin, entities.RoleSystem))

	operationGroup := systemGroup.Group("operation")
	assetOperationsGroup := operationGroup.Group("asset")
	assetOperationsGroup.POST("/add", apiPostHandler(CreateAssetAdminOperationHandler, &systemHandler))
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
		v.RegisterValidation("wei_amount", schema.WeiAmountCheck)
		v.RegisterValidation("token_symbol_check", schema.TokenSymbolCheck)
		v.RegisterValidation("calldata_hex", schema.CalldataHexCheck)
		v.RegisterStructValidation(schema.ValidatePlaceOrderRequest, schema.PlaceOrderRequest{})
		v.RegisterStructValidation(schema.ValidateCancelOrderRequest, schema.CancelOrderRequest{})
		v.RegisterStructValidation(schema.ValidateWithdrawNativeRequest, schema.WithdrawNativeRequest{})
	}
	return nil
}
