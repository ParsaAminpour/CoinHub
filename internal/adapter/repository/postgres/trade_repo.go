package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"gorm.io/gorm"
)

type TradeRepository struct {
	db *gorm.DB
}

func NewTradeRepository(db *gorm.DB) repositories.TradeRepository {
	return &TradeRepository{db: db}
}

func (tr *TradeRepository) CreateTrade(ctx context.Context, trade *entities.Trade) error {
	return tr.db.WithContext(ctx).Create(trade).Error
}

func (tr *TradeRepository) GetTradeByID(ctx context.Context, tradeID string) (*entities.Trade, error) {
	var trade entities.Trade
	if err := tr.db.WithContext(ctx).Where("id = ?", tradeID).First(&trade).Error; err != nil {
		return nil, err
	}
	return &trade, nil
}

func (tr *TradeRepository) GetTradesByPair(ctx context.Context, pair string) ([]*entities.Trade, error) {
	var trades []*entities.Trade
	if err := tr.db.WithContext(ctx).Where("pair = ?", pair).Order("executed_at desc").Find(&trades).Error; err != nil {
		return nil, err
	}
	return trades, nil
}

func (tr *TradeRepository) GetTradesByOrderID(ctx context.Context, orderID string) ([]*entities.Trade, error) {
	var trades []*entities.Trade
	if err := tr.db.WithContext(ctx).
		Where("maker_order_id = ? OR taker_order_id = ?", orderID, orderID).
		Order("executed_at desc").
		Find(&trades).Error; err != nil {
		return nil, err
	}
	return trades, nil
}
