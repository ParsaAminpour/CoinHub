package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func (or *OrderRepository) GetOrderByID(ctx context.Context, orderId uuid.UUID) (*entities.Order, error) {
	var order entities.Order
	if err := or.db.Model(entities.Order{}).Where("ID = ?", orderId).First(&order).Error; err != nil {
		return nil, err
	}
	return &order, nil
}

func (or *OrderRepository) UpdateOrderStatus(ctx context.Context, orderID string, status entities.OrderStatus, filled decimal.Decimal) error {
	return or.db.WithContext(ctx).
		Model(&entities.Order{}).
		Where("id = ?", orderID).
		Updates(map[string]any{
			"status": status,
			"filled": filled,
		}).Error
}

func (or *OrderRepository) MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error) {
	if eventID == "" {
		return false, errors.New("eventID is required")
	}
	record := entities.NewProcessedOrderEvent(eventID, consumerName)
	tx := or.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "event_id"}},
		DoNothing: true,
	}).Create(&record)
	if tx.Error != nil {
		return false, tx.Error
	}
	if tx.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}
