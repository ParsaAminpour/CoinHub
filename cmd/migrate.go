package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/repository/postgres"
	"coinhub/internal/domain/entities"
	"coinhub/internal/infrastructure/configs"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func migrateDatabase(app *internal.Application) error {
	if app.MySqlGorm != nil && app.Configs.App.Env != "PRODUCTION" && app.Configs.App.Debug {
		// Drop all tables before recreating so stale column types never block migration.
		// We use the model structs so GORM resolves each TableName() correctly,
		// then issue a single DROP ... CASCADE to handle FK dependencies in one pass.
		dropModels := []interface{}{
			&entities.Trade{},
			&entities.ProcessedOrderEvent{},
			&entities.Order{},
			&entities.TransferEvent{},
			&entities.EvmTransaction{},
			&entities.AssetBalance{},
			&entities.WalletAccount{},
			&entities.TradingPair{},
			&entities.Asset{},
			&entities.Profile{},
			&entities.User{},
			&entities.Role{},
		}
		tableNames := make([]string, 0, len(dropModels))
		for _, m := range dropModels {
			stmt := &gorm.Statement{DB: app.MySqlGorm}
			if err := stmt.Parse(m); err == nil {
				tableNames = append(tableNames, `"`+stmt.Table+`"`)
			}
		}

		mgModels := []interface{}{
			&entities.Role{}, // must migrate before User (FK: users.role_id → roles.id)
			&entities.User{},
			&entities.Profile{},
			&entities.Asset{},
			&entities.TradingPair{},
			&entities.WalletAccount{},
			&entities.EvmTransaction{},
			&entities.TransferEvent{},
			&entities.Order{},
			&entities.ProcessedOrderEvent{},
			&entities.Trade{},
		}

		zap.S().Debugw("starting migrations", zap.Int("count", len(mgModels)))
		if err := app.MySqlGorm.AutoMigrate(mgModels...); err != nil {
			zap.S().Errorw("failed to migrate database", zap.Error(err))
			return errors.Join(errors.New("failed to migrate database"), err)
		}

		zap.S().Info("All migrations completed successfully!")
		return nil
	}
	return nil
}

func seedDatabase(app *internal.Application) error {
	seedsDir := "internal/infrastructure/database/seeds"
	files, err := filepath.Glob(filepath.Join(seedsDir, "*.sql"))
	if err != nil {
		return fmt.Errorf("failed to read seeds directory: %w", err)
	}

	if len(files) == 0 {
		zap.S().Warn("No seed files found in db/seeds directory")
		return nil
	}

	for _, file := range files {
		zap.S().Infow("Applying seed file", "file", file)

		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read seed file %s: %w", file, err)
		}

		if err := app.MySqlGorm.Exec(string(content)).Error; err != nil {
			return fmt.Errorf("failed to execute seed file %s: %w", file, err)
		}
	}

	zap.S().Info("All seed files applied successfully")
	return nil
}

func RunMigrate(configs *configs.Configuration) *cobra.Command {
	var (
		upFlag   bool
		seedFlag bool
	)
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run the migrations",
		Long:  "Run the migrations",
		Run: func(cmd *cobra.Command, args []string) {
			if !upFlag && !seedFlag {
				zap.S().Error("Please specify one of: --up or --seed")
				if err := cmd.Help(); err != nil {
					zap.S().Error("failed to show help", zap.Error(err))
					os.Exit(1)
				}
				os.Exit(1)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			app := internal.NewApplication(ctx, configs, internal.ApplicationOptions{
				CommandName:       cmd.Name(),
				SkipHDWallet:      true,
				SkipRedis:         true,
				SkipRepositories:  true,
				SkipETHClient:     true,
				SkipWalletService: true,
				SkipAsynq:         true,
				SkipMail:          true,
				SkipCache:         true,
				SkipMatchEngine:   true,
				SkipMessageBroker: true,
			})

			if upFlag {
				zap.S().Info("migrating database")
				if err := migrateDatabase(app); err != nil {
					zap.S().Error("failed to migrate database", zap.Error(err))
				}
				zap.S().Info("seeding primary roles")
				if err := postgres.NewRoleRepository(app.MySqlGorm).Seed(ctx); err != nil {
					zap.S().Error("failed to seed roles", zap.Error(err))
				}
			}

			if seedFlag {
				zap.S().Info("seeding database")
				if err := seedDatabase(app); err != nil {
					zap.S().Error("failed to seed database", zap.Error(err))
				}
			}
		},
	}

	cmd.Flags().BoolVar(&upFlag, "up", false, "Apply migrations up to latest or specified version")
	cmd.Flags().BoolVar(&seedFlag, "seed", false, "Apply seed files to the database")
	return cmd
}
