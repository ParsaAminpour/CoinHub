package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type TradeRepository interface {
	CreateTrade(ctx context.Context, trade *entities.Trade) error
	GetTradeByID(ctx context.Context, tradeID string) (*entities.Trade, error)
	GetTradesByPair(ctx context.Context, pair string) ([]*entities.Trade, error)
	GetTradesByOrderID(ctx context.Context, orderID string) ([]*entities.Trade, error)
}
