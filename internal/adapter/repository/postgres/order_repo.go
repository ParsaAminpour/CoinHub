package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type OrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) repositories.OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (or *OrderRepository) CreateOrder(ctx context.Context, order *entities.Order) error {
	if err := or.db.Model(entities.Order{}).Create(order).Error; err != nil {
		return err
	}
	return nil
}

func (or *OrderRepository) GetOrderByID(ctx context.Context, orderId uuid.UUID) error {
	var order entities.Order
	if err := or.db.Model(entities.Order{}).Where("ID = ?", orderId).First(&order).Error; err != nil {
		return err
	}
	return nil
}
