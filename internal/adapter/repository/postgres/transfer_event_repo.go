package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"gorm.io/gorm"
)

type TransferEventRepository struct {
	db *gorm.DB
}

func NewTransferEventRepository(db *gorm.DB) repositories.TransferEventRepository {
	return TransferEventRepository{db: db}
}

func (t TransferEventRepository) Create(ctx context.Context, evmTransfer *entities.TransferEvent) error {
	if evmTransfer == nil {
		return nil
	}
	result := t.db.WithContext(ctx).Create(evmTransfer)
	return result.Error
}
