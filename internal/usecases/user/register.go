package user

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
)

// Contains What actions can users perform
type RegisterUserUsecases struct {
	userRepo repositories.UserRepository
}

func NewRegisterUserUsecases(repo repositories.UserRepository) RegisterUserUsecases {
	return RegisterUserUsecases{userRepo: repo}
}

func (r *RegisterUserUsecases) Register(ctx context.Context, user *entities.User) error {
	// implement here...
	return nil
}
