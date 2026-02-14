package user

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"go.uber.org/zap"
)

// Contains What actions can users perform
type RegisterUserUsecases struct {
	userRepo repositories.UserRepository
}

// repo is injected from entities:repositories
func NewRegisterUserUsecases(repo repositories.UserRepository) RegisterUserUsecases {
	return RegisterUserUsecases{userRepo: repo}
}

func (r *RegisterUserUsecases) Register(ctx context.Context, user *entities.User) error {
	if err := r.userRepo.Create(ctx, user); err != nil {
		return err
	}
	zap.S().Infow("user created", "user", user.ID)
	return nil
}
