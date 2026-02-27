package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type AssetRepository struct {
	db *gorm.DB
}

func NewAssetRepository(db *gorm.DB) repositories.AssetRepository {
	return AssetRepository{db: db}
}

func (ass AssetRepository) GetAssetByCotnractAddress(ctx context.Context, asset *entities.Asset, ca string) error {
	if err := ass.db.WithContext(ctx).Where("asset_address = ?", &ca).First(asset).Error; err != nil {
		zap.S().Errorw("failed to get asset by contract address", "error", err, "contract_address", ca)
		return err
	}
	return nil
}

func (ass AssetRepository) Create(ctx context.Context, asset *entities.Asset) error {
	return nil
}
