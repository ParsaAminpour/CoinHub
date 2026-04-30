package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type RoleRepository struct {
	db *gorm.DB
}

func NewRoleRepository(db *gorm.DB) repositories.RoleRepository {
	return &RoleRepository{db: db}
}

func (r *RoleRepository) GetByName(ctx context.Context, name entities.RoleName) (*entities.Role, error) {
	var role entities.Role
	if err := r.db.WithContext(ctx).Where("name = ?", name).First(&role).Error; err != nil {
		return nil, err
	}
	return &role, nil
}

func (r *RoleRepository) GetByID(ctx context.Context, id uint) (*entities.Role, error) {
	var role entities.Role
	if err := r.db.WithContext(ctx).First(&role, id).Error; err != nil {
		return nil, err
	}
	return &role, nil
}

// Seed inserts the primary roles if they do not already exist.
func (r *RoleRepository) Seed(ctx context.Context) error {
	primaryRoles := []entities.Role{
		{Name: entities.RoleUser},
		{Name: entities.RoleAdmin},
		{Name: entities.RoleSystem},
	}

	if err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&primaryRoles).Error; err != nil {
		zap.S().Errorw("failed to seed roles", "error", err)
		return err
	}

	zap.S().Infow("roles seeded", "count", len(primaryRoles))
	return nil
}
