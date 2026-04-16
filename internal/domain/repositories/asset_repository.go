package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type AssetRepository interface {
	Create(ctx context.Context, asset *entities.Asset) error
	GetAssetByCotnractAddress(ctx context.Context, ca string) (*entities.Asset, error)
	GetAvailableAssets(ctx context.Context) ([]entities.Asset, error)
	GetAvailableAssetsCount(ctx context.Context) (int, error)
}
