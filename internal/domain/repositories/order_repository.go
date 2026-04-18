package repositories

import (
	"coinhub/internal/domain/entities"
	"context"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type OrderRepository interface {
	CreateOrder(ctx context.Context, order *entities.Order) error
	GetOrderByID(ctx context.Context, orderId uuid.UUID) (*entities.Order, error)
	UpdateOrderStatus(ctx context.Context, orderID string, status entities.OrderStatus, filled decimal.Decimal) error
}
