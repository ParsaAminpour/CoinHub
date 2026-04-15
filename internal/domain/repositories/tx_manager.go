package repositories

import (
	"context"
)

type Tx interface {
	Users() UserRepository
	Wallets() WalletAccountRepository
	Orders() OrderRepository
	// other repositories will register here if it needed..
}

type TxManager interface {
	WithinTransaction(ctx context.Context, handler func(ctx context.Context, tx Tx) error) error
}
