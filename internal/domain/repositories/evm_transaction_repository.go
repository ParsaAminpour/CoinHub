package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type EVMTransactionRepository interface {
	CreateTransaction(ctx context.Context, evmTx *entities.EvmTransaction) error
	GetTransactionByHash(ctx context.Context, hash string) error
}
