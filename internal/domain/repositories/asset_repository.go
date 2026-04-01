package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type AssetRepository interface {
	Create(ctx context.Context, asset *entities.Asset) error
	GetAssetByCotnractAddress(ctx context.Context, ca string) (*entities.Asset, error)
}
