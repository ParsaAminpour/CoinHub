package internal

import (
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/database"
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Application struct {
	Configs *configs.Configuration

	Redis     *redis.Client // add it later
	MySqlGorm *gorm.DB
}

func NewApplication(ctx context.Context) Application {
	if err := configs.LoadConfig(".env"); err != nil {
		zap.S().Fatalw("unable to load configuration")
	}
	app := Application{Configs: &configs.C}

	if err := app.RegisterMySqlGorm(); err != nil {
		zap.S().Fatalw("error in registering DB: %s", err.Error())
	}

	if err := app.RegisterRedis(); err != nil {
		zap.S().Fatalw("error in registering redis: %s", err.Error())
	}
	return app
}

func (app *Application) RegisterMySqlGorm() error {
	db, err := database.NewMySqlDB(app.Configs.Storage.DatabaseUrl)
	if err != nil {
		return fmt.Errorf("%s", err.Error())
	}
	app.MySqlGorm = db
	return nil
}

func (app *Application) RegisterRedis() error {
	return nil
}
