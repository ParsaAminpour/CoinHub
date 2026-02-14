package swagger

import (
	"coinhub/docs"
	"coinhub/internal/infrastructure/configs"
)

func InitSwagger(cfg *configs.Configuration) error {
	docs.SwaggerInfo.Title = "Coinhub-API-Service"
	docs.SwaggerInfo.Description = "Coinhub Centralized Exchange"
	docs.SwaggerInfo.Version = cfg.App.Version
	return nil
}
