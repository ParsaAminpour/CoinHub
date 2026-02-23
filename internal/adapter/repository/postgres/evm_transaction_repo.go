package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"gorm.io/gorm"
)

type EVMTransactionRepository struct {
	db *gorm.DB
}

func NewEVMTransactionRepository(db *gorm.DB) repositories.EVMTransactionRepository {
	return &EVMTransactionRepository{db: db}
}

func (r *EVMTransactionRepository) CreateTransaction(ctx context.Context, evmTx *entities.EvmTransaction) error {
	if evmTx == nil {
		return nil
	}
	var user entities.User
	if err := r.db.WithContext(ctx).First(&user, "id = ?", evmTx.UserID).Error; err != nil {
		return err
	}
	result := r.db.WithContext(ctx).Create(evmTx)
	return result.Error
}

func (r *EVMTransactionRepository) GetTransactionByHash(ctx context.Context, evmTx *entities.EvmTransaction, hash string) error {
	if hash == "" {
		return gorm.ErrRecordNotFound
	}

	result := r.db.WithContext(ctx).Where("hash = ?", hash).First(&evmTx)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func (r *EVMTransactionRepository) UpdateTransactionStatus(ctx context.Context, trxHash string, newStatus entities.TransactionStatus) error {
	if err := r.db.WithContext(ctx).
		Model(&entities.EvmTransaction{}).
		Where("hash = ?", trxHash).
		Update("status", newStatus).Error; err != nil {
		return err
	}
	return nil
}
