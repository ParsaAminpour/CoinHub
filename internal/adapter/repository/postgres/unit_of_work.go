package postgres

import (
	"coinhub/internal/domain/repositories"
	"context"

	"gorm.io/gorm"
)

type GormUnitOfWork struct {
	db *gorm.DB
}

func NewGormUnitOfWork(db *gorm.DB) repositories.TxManager {
	return GormUnitOfWork{db: db}
}

// NOTE : the handler handled an orchestration among registered repositories for an atomic operation.
func (uow GormUnitOfWork) WithinTransaction(ctx context.Context, handler func(ctx context.Context, tx repositories.Tx) error) error {
	return uow.db.WithContext(ctx).Transaction(func(_tx *gorm.DB) error {
		tx := &txContext{
			userRepo:   NewUserRepository(_tx),
			walletRepo: NewWalletRepository(_tx),
			orederRepo: NewOrderRepository(_tx),
		}
		return handler(ctx, tx)
	})
}

// txContext follows repositoties::Tx interface
type txContext struct {
	userRepo   repositories.UserRepository
	walletRepo repositories.WalletAccountRepository
	orederRepo repositories.OrderRepository
}

func (t *txContext) Users() repositories.UserRepository            { return t.userRepo }
func (t *txContext) Wallets() repositories.WalletAccountRepository { return t.walletRepo }
func (t *txContext) Orders() repositories.OrderRepository          { return t.orederRepo }
