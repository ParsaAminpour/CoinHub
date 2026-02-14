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

func NewUserRepository(db *gorm.DB) repositories.UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, user *entities.User) error {
	if err := r.db.Create(&user).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) Delete(ctx context.Context, userId uuid.UUID) error {
	if err := r.db.Where("user_id = ?", userId).Delete(ctx).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) Update(ctx context.Context, userId uuid.UUID) error {
	// Implement here...
	return nil
}
