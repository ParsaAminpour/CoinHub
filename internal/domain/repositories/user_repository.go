package repositories

import (
	"coinhub/internal/domain/entities"
	"context"

	"github.com/google/uuid"
)

type UserRepository interface {
	Create(ctx context.Context, user *entities.User) error
	Delete(ctx context.Context, userId uuid.UUID) error
	Update(ctx context.Context, userId uuid.UUID) error
	GetUserByID(ctx context.Context, user *entities.User, userId uuid.UUID) error
	GetUserByUsername(ctx context.Context, user *entities.User, username string) error
	GetUserByGmail(ctx context.Context, user *entities.User, gmail string) error
	GetUserByWalletAccount(ctx context.Context, user *entities.User, walletAddress string) error
}
