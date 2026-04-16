package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type TradingPairRepository struct {
	db *gorm.DB
}

func NewTradingPairRepository(db *gorm.DB) repositories.TradingPairRepository {
	return &TradingPairRepository{db: db}
}

func (r *TradingPairRepository) Create(ctx context.Context, pair *entities.TradingPair) error {
	if err := r.db.WithContext(ctx).Create(pair).Error; err != nil {
		zap.S().Errorw("failed to create trading pair", "error", err, "pair", pair)
		return err
	}
	return nil
}

func (r *TradingPairRepository) GetByID(ctx context.Context, id uuid.UUID) (*entities.TradingPair, error) {
	var pair entities.TradingPair
	if err := r.db.WithContext(ctx).
		Preload("BaseAsset").
		Preload("QuoteAsset").
		Where("id = ?", id).
		First(&pair).Error; err != nil {
		return nil, err
	}
	return &pair, nil
}

func (r *TradingPairRepository) GetBySymbol(ctx context.Context, baseAssetID, quoteAssetID uuid.UUID) (*entities.TradingPair, error) {
	var pair entities.TradingPair
	if err := r.db.WithContext(ctx).
		Preload("BaseAsset").
		Preload("QuoteAsset").
		Where("base_asset_id = ? AND quote_asset_id = ?", baseAssetID, quoteAssetID).
		First(&pair).Error; err != nil {
		return nil, err
	}
	return &pair, nil
}

func (r *TradingPairRepository) GetActivePairs(ctx context.Context) ([]entities.TradingPair, error) {
	var pairs []entities.TradingPair
	if err := r.db.WithContext(ctx).
		Preload("BaseAsset").
		Preload("QuoteAsset").
		Where("status = ?", entities.TradingPairStatusActive).
		Find(&pairs).Error; err != nil {
		return nil, err
	}
	return pairs, nil
}

func (r *TradingPairRepository) GetActivePairsCount(ctx context.Context) (int32, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&entities.TradingPair{}).
		Where("status = ?", entities.TradingPairStatusActive).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return int32(count), nil
}

func (r *TradingPairRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status entities.TradingPairStatus) error {
	return r.db.WithContext(ctx).
		Model(&entities.TradingPair{}).
		Where("id = ?", id).
		Update("status", status).Error
}
