package user_usecases

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"context"

	"go.uber.org/zap"
)

// Contains What actions can users perform
type RegisterUserUsecases struct {
	txManager repositories.TxManager
}

// repo is injected from entities:repositories
func NewRegisterUserUsecases(txManager repositories.TxManager) RegisterUserUsecases {
	return RegisterUserUsecases{txManager: txManager}
}

func (r *RegisterUserUsecases) Register(ctx context.Context, walletService services.WalletService, user *entities.User) error {
	return r.txManager.WithinTransaction(ctx, func(ctx context.Context, tx repositories.Tx) error {
		if err := tx.Users().Create(ctx, user); err != nil {
			zap.S().Errorw("Failed to create user during registration", "error", err, "username", user.Username)
			return err
		}
		if err := tx.Wallets().CreateNewWallet(ctx, walletService, user.ID); err != nil {
			zap.S().Errorw("Failed to create wallet during registration", "error", err, "user_id", user.ID, "username", user.Username)
			return err
		}
		zap.S().Infow("User registration transaction steps completed",
			"user_id", user.ID,
			"username", user.Username,
			"wallet_created", true,
		)
		return nil
	})
}
