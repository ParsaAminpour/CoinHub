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
}
