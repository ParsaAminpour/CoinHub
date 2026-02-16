package internal

import (
	repository "coinhub/internal/adapter/repository/postgres"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/database"
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-redis/redis/v8"
	hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Application struct {
	Configs *configs.Configuration

	UserRepository          repositories.UserRepository
	WalletAccountRepository repositories.WalletAccountRepository
	WalletService           services.WalletService // access hdWallet with its service

	Redis     *redis.Client // add it later
	MySqlGorm *gorm.DB

	ETHClient *ethclient.Client

	hdWallet *hdwallet.Wallet // private - only accessible through WalletService
}

func NewApplication(ctx context.Context, configs *configs.Configuration) Application {
	app := Application{Configs: configs}

	if err := app.registerHDWallet(); err != nil {
		zap.S().Fatalw("error in registering HDWallet: %s", err.Error())
	}

	zap.S().Info("HD wallet initialized and secured")

	if err := app.registerMySqlGorm(); err != nil {
		zap.S().Fatalw("error in registering DB: %s", err.Error())
	}

	if err := app.registerRedis(); err != nil {
		zap.S().Fatalw("error in registering redis: %s", err.Error())
	}

	if err := app.registerRepositories(); err != nil {
		zap.S().Fatalw("error in registering repositories: %s", err.Error())
	}

	if err := app.registerETHClient(); err != nil {
		zap.S().Fatalw("error in registering eth client: %s", err.Error())
	}

	if err := app.registerServices(); err != nil {
		zap.S().Fatalw("error in registering services: %s", err.Error())
	}

	return app
}

func (app *Application) registerRepositories() error {
	app.UserRepository = repository.NewUserRepository(app.MySqlGorm)
	app.WalletAccountRepository = repository.NewWalletRepository(app.MySqlGorm)
	return nil
}

func (app *Application) registerMySqlGorm() error {
	db, err := database.NewMySqlDB(app.Configs.Storage.DatabaseUrl)
	if err != nil {
		return fmt.Errorf("%s", err.Error())
	}
	app.MySqlGorm = db
	return nil
}

func (app *Application) registerHDWallet() error {
	wallet, err := hdwallet.NewFromMnemonic(app.Configs.App.HDWalletMnemonic)
	if err != nil {
		return err
	}
	app.hdWallet = wallet
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
	return nil
}

func (app *Application) registerServices() error {
	// WalletService encapsulates HDWallet access
	app.WalletService = services.NewWalletService(app.hdWallet, app.ETHClient)
	return nil
}

func (app *Application) registerRedis() error {
	return nil
}
