package internal

import (
	"coinhub/internal/adapter/repository/cache"
	repository "coinhub/internal/adapter/repository/postgres"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/database"
	"context"
	"flag"
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/hibiken/asynq"
	hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
	"gorm.io/gorm"
)

// TODO : Add connections graceful shutdown
type Application struct {
	Configs *configs.Configuration

	UserRepository          repositories.UserRepository
	WalletAccountRepository repositories.WalletAccountRepository
	TransactinRepository    repositories.EVMTransactionRepository
	TxManager               repositories.TxManager

	WalletService services.WalletService // access hdWallet and ethclient with its service

	MailDialer  *gomail.Dialer
	RedisClient *redis.Client // add it later
	MySqlGorm   *gorm.DB

	ETHClient *ethclient.Client

	hdWallet *hdwallet.Wallet // private - only accessible through WalletService

	AsynqClient    *asynq.Client
	AsynqInspector *asynq.Inspector
	AsynqServer    *asynq.Server

	WsClient *websocket.Conn

	AuthGmailCache *cache.AuthGmailCache
}

func NewApplication(ctx context.Context, configs *configs.Configuration) Application {
	app := Application{Configs: configs}

	if err := app.registerHDWallet(); err != nil {
		zap.S().Fatalw("error in registering HDWallet: %s", err.Error())
	}

	zap.S().Info("HD wallet initialized and secured")

	if err := app.registerMySqlGorm(); err != nil {
		zap.S().Fatalw("❌ error in registering DB: %s", err.Error())
	}

	if err := app.registerRedis(ctx); err != nil {
		zap.S().Fatalw("❌ error in registering redis: %s", err.Error())
	}

	if err := app.registerRepositories(); err != nil {
		zap.S().Fatalw("❌ error in registering repositories: %s", err.Error())
	}

	if err := app.registerETHClient(); err != nil {
		zap.S().Fatalw("❌ error in registering eth client: %s", err.Error())
	}

	if err := app.registerServices(); err != nil {
		zap.S().Fatalw("❌ error in registering services: %s", err.Error())
	}

	if err := app.registerAsynqClient(); err != nil {
		zap.S().Fatalw("❌ error in registering asynq client: %s", err.Error())
	}

	if err := app.registerWebsocketClient(ctx, configs.App.WSClientEthereumTestnet, configs.App.NetworkStatus); err != nil {
		zap.S().Fatalw(
			"❌ Failed to register websocket client. Configuration: address=%s, network_status=%s, error=%s",
			configs.App.WSClientEthereumTestnet,
			configs.App.NetworkStatus,
			err.Error(),
		)
	}

	if err := app.registerMailDialer(ctx); err != nil {
		zap.S().Fatalw("❌ error in registering mail dialer: %s", err.Error())
	}

	if err := app.registerCache(ctx); err != nil {
		zap.S().Fatalw("❌ error in registering cache: %s", err.Error())
	}

	return app
}

func (app *Application) registerCache(ctx context.Context) error {
	if _, err := app.RedisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("cache configuration failed because redis client has not registered yet")
	}
	app.AuthGmailCache = cache.NewAuthGmailCache(ctx, app.RedisClient, tasks.EMAIL_VERIFICATION_CODE_LIFETIME_DURATION)
	zap.S().Info("AuthGmailCache initialized and registered with Redis ✅")
	return nil
}

func (app *Application) registerAsynqClient() error {
	app.AsynqClient = asynq.NewClient(asynq.RedisClientOpt{
		Addr:     app.Configs.RedisAddress(),
		Password: app.Configs.Storage.Redis.Password,
		DB:       app.Configs.Service.QueueDB,
	})
	app.AsynqInspector = asynq.NewInspector(asynq.RedisClientOpt{
		Addr:     app.Configs.RedisAddress(),
		Password: app.Configs.Storage.Redis.Password,
		DB:       app.Configs.Service.QueueDB,
	})
	zap.S().Infow("Asynq client and inspector registered ✅")
	return nil
}

func (app *Application) registerRepositories() error {
	app.UserRepository = repository.NewUserRepository(app.MySqlGorm)
	app.WalletAccountRepository = repository.NewWalletRepository(app.MySqlGorm)
	app.TransactinRepository = repository.NewEVMTransactionRepository(app.MySqlGorm)
	app.TxManager = repository.NewGormUnitOfWork(app.MySqlGorm)
	zap.S().Infow("Repositories registered ✅")
	return nil
}

func (app *Application) registerMySqlGorm() error {
	db, err := database.NewMySqlDB(app.Configs.Storage.DatabaseUrl)
	if err != nil {
		return fmt.Errorf("%s", err.Error())
	}
	app.MySqlGorm = db
	zap.S().Infow("MySQL GORM database registered ✅")
	return nil
}

func (app *Application) registerHDWallet() error {
	wallet, err := hdwallet.NewFromMnemonic(app.Configs.App.HDWalletMnemonic)
	if err != nil {
		return err
	}
	app.hdWallet = wallet
	zap.S().Infow("HDWallet registered ✅")
	return nil
}

func (app *Application) registerETHClient() error {
	var baseUrl string
	if app.Configs.App.NetworkStatus == "MAINNET" {
		baseUrl = app.Configs.App.ETHClientMainnet
	} else {
		baseUrl = app.Configs.App.ETHClientTestnet
	}
	client, err := ethclient.Dial(baseUrl)
	if err != nil {
		return err
	}
	app.ETHClient = client
	zap.S().Infow("ETHClient registered ✅")
	return nil
}

func (app *Application) registerServices() error {
	// WalletService encapsulates HDWallet access
	app.WalletService = services.NewWalletService(app.hdWallet, app.ETHClient)
	return nil
}

func (app *Application) registerRedis(ctx context.Context) error {

	client := redis.NewClient(&redis.Options{
		Addr:     app.Configs.RedisAddress(),
		Password: app.Configs.Storage.Redis.Password,
		DB:       app.Configs.Service.CacheDB,
	})

	if _, err := client.Ping(ctx).Result(); err != nil {
		return err
	}
	app.RedisClient = client
	zap.S().Infow("Redis client registered ✅", "address", app.Configs.RedisAddress())
	return nil
}

func (app *Application) registerWebsocketClient(ctx context.Context, clientUrl string, network string) error {
	addr := flag.String("wsClientAddr", clientUrl, fmt.Sprintf("Network"))
	// u := url.URL{Scheme: "ws", Host: *addr, Path: "/echo"}
	c, _, err := websocket.DefaultDialer.Dial(*addr, nil)
	if err != nil {
		return err
	}
	app.WsClient = c
	zap.S().Infow("Websocket client registered ✅")
	return nil
}

func (app *Application) registerMailDialer(ctx context.Context) error {
	app.MailDialer = gomail.NewDialer(
		app.Configs.Mail.SMTPHost,
		app.Configs.Mail.SMTPPort,
		app.Configs.Mail.SMTPUsername,
		app.Configs.Mail.SMTPPassword,
	)
	zap.S().Infow("Mail dialer config", "host", app.Configs.Mail.SMTPHost, "port", app.Configs.Mail.SMTPPort, "username", app.Configs.Mail.SMTPUsername)
	zap.S().Info("Mail dialer registered✅")
	return nil
}
