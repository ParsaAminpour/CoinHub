package internal

import (
	repository "coinhub/internal/adapter/repository/postgres"
	"coinhub/internal/domain/repositories"
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

	UserRepository repositories.UserRepository

	Redis     *redis.Client // add it later
	MySqlGorm *gorm.DB
}

func NewApplication(ctx context.Context, configs *configs.Configuration) Application {
	app := Application{Configs: configs}

	if err := app.registerMySqlGorm(); err != nil {
		zap.S().Fatalw("error in registering DB: %s", err.Error())
	}

	if err := app.registerRedis(); err != nil {
		zap.S().Fatalw("error in registering redis: %s", err.Error())
	}

	if err := app.registerRepositories(); err != nil {
		zap.S().Fatalw("error in registering repositories: %s", err.Error())
	}
	return app
}

func (app *Application) registerRepositories() error {
	app.UserRepository = repository.NewUserRepository(app.MySqlGorm)
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

func (app *Application) registerRedis() error {
	return nil
}
