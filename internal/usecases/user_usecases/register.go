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
	userRepo   repositories.UserRepository
	walletRepo repositories.WalletAccountRepository
}

// repo is injected from entities:repositories
func NewRegisterUserUsecases(userRepo repositories.UserRepository, walletRepo repositories.WalletAccountRepository) RegisterUserUsecases {
	return RegisterUserUsecases{userRepo: userRepo, walletRepo: walletRepo}
}

// TODO : make this atomic, but how with this architecture??
func (r *RegisterUserUsecases) Register(ctx context.Context, walletService services.WalletService, user *entities.User) error {
	if err := r.userRepo.Create(ctx, user); err != nil {
		return err
	}
	zap.S().Infow("Creating user", "user_id", user.ID)
	walletAddress, err := r.walletRepo.CreateNewWallet(ctx, walletService, user.ID)
	if err != nil {
		return err
	}

	zap.S().Infow("user created", "user", user.ID)
	zap.S().Infow("wallet generated for user", "walletAddress", walletAddress)
	return nil
}
