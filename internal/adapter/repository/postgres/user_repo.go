package repository

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type UserRepository struct {
	db *gorm.DB
}

// Implement the domain:repositories interfaces here..
func NewUserRepository(db *gorm.DB) repositories.UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, user *entities.User) error {
	return r.db.WithContext(ctx).Create(user).Error
}

func (r *UserRepository) Delete(ctx context.Context, userId uuid.UUID) error {
	return r.db.WithContext(ctx).Delete(&entities.User{}, userId).Error
}

func (r *UserRepository) Update(ctx context.Context, userId uuid.UUID) error {
	return nil
}
