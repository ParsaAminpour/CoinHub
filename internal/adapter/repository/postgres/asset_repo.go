package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"fmt"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type AssetRepository struct {
	db *gorm.DB
}

func NewAssetRepository(db *gorm.DB) repositories.AssetRepository {
	return &AssetRepository{db: db}
}

func (ass *AssetRepository) GetAvailableAssetsCount(ctx context.Context) (int, error) {
	var count int64
	if err := ass.db.Model(&entities.Asset{}).Where("status = ?", entities.AssetStatusActive).Count(&count).Error; err != nil {
		return 0, err
	}
	return int(count), nil
}

func (ass *AssetRepository) GetAvailableAssets(ctx context.Context) ([]entities.Asset, error) {
	var availableAssets []entities.Asset
	if err := ass.db.Model(&entities.Asset{}).Where("status = ?", entities.AssetStatusActive).Find(&availableAssets).Error; err != nil {
		return nil, err
	}
	return availableAssets, nil
}

// TODO : refactor this functoin
func (ass *AssetRepository) GetAssetByCotnractAddress(ctx context.Context, ca string) (*entities.Asset, error) {
	allAssets := []entities.Asset{}
	if err := ass.db.Model(entities.Asset{}).WithContext(ctx).Where("asset_address = ?", ca).Find(&allAssets).Error; err != nil {
		zap.S().Errorw("failed to get all assets by contract address", "error", err, "contract_address", ca)
		return nil, err
	}
	zap.S().Infow("Found assets", "assets", allAssets)
	if len(allAssets) == 0 {
		return nil, fmt.Errorf("asset not found")
	}

	return &allAssets[0], nil
}

func (ass *AssetRepository) GetActiveTradingPairsCount(ctx context.Context) (int32, error) {
	var count int64
	if err := ass.db.Model(&entities.TradingPair{}).
		Where("status = ?", entities.TradingPairStatusActive).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return int32(count), nil
}

func (ass *AssetRepository) Create(ctx context.Context, asset *entities.Asset) error {
	if err := ass.db.WithContext(ctx).Create(asset).Error; err != nil {
		zap.S().Errorw("failed to create asset", "error", err, "asset", asset)
		return err
	}
	return nil
}
