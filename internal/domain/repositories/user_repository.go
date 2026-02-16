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
	GetUserByID(ctx context.Context, userId uuid.UUID) (*entities.User, error)
	GetUserByUsername(ctx context.Context, username string) (*entities.User, error)
	GetUserByGmail(ctx context.Context, gmail string) (*entities.User, error)
}
