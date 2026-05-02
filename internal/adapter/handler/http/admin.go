package http

import (
	"html/template"

	"coinhub/internal/infrastructure/configs"

	"github.com/GoAdminGroup/go-admin/engine"
	"github.com/GoAdminGroup/go-admin/modules/config"
	"github.com/GoAdminGroup/go-admin/modules/db"
	"github.com/GoAdminGroup/go-admin/plugins/admin"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	_ "github.com/GoAdminGroup/go-admin/adapter/gin"
	_ "github.com/GoAdminGroup/go-admin/modules/db/drivers/postgres"
	_ "github.com/GoAdminGroup/themes/adminlte"
)

// SetupAdminPanel mounts the GoAdmin panel at /admin on the provided Gin engine.
// It uses the application's existing PostgreSQL database.
//
// NOTE: GoAdmin requires its own schema tables to exist before first run.
// Apply the official migration SQL from:
// https://github.com/GoAdminGroup/go-admin/tree/master/data
func SetupAdminPanel(router *gin.Engine, cfg *configs.Configuration) error {
	eng := engine.Default()

	zap.S().Infow("Setting up admin panel", "database_url", cfg.Storage.DatabaseUrl)

	adminCfg := config.Config{
		Databases: config.DatabaseList{
			"default": {
				Driver: db.DriverPostgresql,
				Dsn:    cfg.Storage.DatabaseUrl,
			},
		},
		UrlPrefix:   "admin",
		Language:    "en",
		Theme:       "adminlte",
		Title:       "CoinHub Admin",
		Logo:        template.HTML("<b>Coin</b>Hub"),
		MiniLogo:    template.HTML("<b>C</b>H"),
		Debug:       cfg.App.Debug,
		ColorScheme: "skin-black",
	}

	return eng.AddConfig(&adminCfg).
		AddPlugins(admin.NewAdmin()).
		Use(router)
}
