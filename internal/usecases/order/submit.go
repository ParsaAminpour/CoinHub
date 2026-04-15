package order_usercases

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/engine"
	"context"

	"github.com/shopspring/decimal"
)

type SubmitOrderUsecases struct {
	txManager repositories.TxManager
}

func NewOrderUsecases(txManager repositories.TxManager) *SubmitOrderUsecases {
	return &SubmitOrderUsecases{
		txManager: txManager,
	}
}

func (ou *SubmitOrderUsecases) SubmitOrder(
	ctx context.Context,
	matchEngine *engine.MatchEngine,
	eventProducer *kafka.OrderEventProducer,
	userID string,
	pair string,
	orderType entities.OrderType,
	side entities.OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal) error {
	// register it on DB
	return ou.txManager.WithinTransaction(ctx, func(ctx context.Context, tx repositories.Tx) error {
		orderEntity := entities.NewOrder(userID, pair, orderType, side, price, quantity)
		if err := tx.Orders().CreateOrder(ctx, orderEntity); err != nil {
			return err
		}

		orderToProcess := engine.NewOrder(userID, pair, engine.OrderType(orderType), engine.OrderSide(side), price, quantity)
		orderToProcess.ID = orderEntity.ID
		if err := matchEngine.SubmitOrder(eventProducer, *orderToProcess); err != nil {
			return err
		}
		// TODO : other updates here:
		return nil
	})
}
