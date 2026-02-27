package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type TransferEventRepository interface {
	Create(ctx context.Context, evmTransfer *entities.TransferEvent) error
}
