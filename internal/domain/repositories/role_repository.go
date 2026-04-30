package repositories

import (
	"coinhub/internal/domain/entities"
	"context"
)

type RoleRepository interface {
	GetByName(ctx context.Context, name entities.RoleName) (*entities.Role, error)
	GetByID(ctx context.Context, id uint) (*entities.Role, error)
	Seed(ctx context.Context) error
}
