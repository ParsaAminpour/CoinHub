package repositories

import (
	"coinhub/internal/domain/entities"
	"context"

	"github.com/google/uuid"
)

type TradingPairRepository interface {
	Create(ctx context.Context, pair *entities.TradingPair) error
	GetByID(ctx context.Context, id uuid.UUID) (*entities.TradingPair, error)
	GetBySymbol(ctx context.Context, baseAssetID, quoteAssetID uuid.UUID) (*entities.TradingPair, error)
	GetActivePairs(ctx context.Context) ([]entities.TradingPair, error)
	GetActivePairsCount(ctx context.Context) (int32, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status entities.TradingPairStatus) error
}
