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
	return &AssetRepository{db: db}
}

func (ass *AssetRepository) GetAssetByCotnractAddress(ctx context.Context, asset *entities.Asset, ca string) error {
	var assets []entities.Asset
	if err := ass.db.WithContext(ctx).Find(&assets).Error; err != nil {
		zap.S().Errorw("failed to fetch assets in GetAssetByCotnractAddress", "error", err)
		return err
	}
	for _, a := range assets {
		zap.S().Infow(
			"Asset",
			"id", a.ID,
			"symbol", a.Symbol,
			"name", a.Name,
			"asset_address", a.AssetAddress,
		)
	}
	if err := ass.db.WithContext(ctx).Where("asset_address = ?", &ca).First(&asset).Error; err != nil {
		zap.S().Errorw("failed to get asset by contract address", "error", err, "contract_address", ca)
		return err
	}
	return nil
}

func (ass *AssetRepository) Create(ctx context.Context, asset *entities.Asset) error {
	return nil
}
