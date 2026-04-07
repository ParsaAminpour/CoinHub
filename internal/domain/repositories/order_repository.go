package repositories

import (
	"coinhub/internal/domain/entities"
	"context"

	"github.com/google/uuid"
)

type OrderRepository interface {
	CreateOrder(ctx context.Context, order *entities.Order) error
	GetOrderByID(ctx context.Context, orderId uuid.UUID) error
}
