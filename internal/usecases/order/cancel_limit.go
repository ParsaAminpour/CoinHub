package order_usercases

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/engine"
	"context"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// NOTE : the existence of txManager here is not useless, we will use it later for other DB operations.
func (cou *OrderUsecases) CancelLimitOrder(
	ctx context.Context,
	matchEngine *engine.MatchEngine,
	eventProducer *kafka.EngineEventProducer,
	userID string,
	pair string, // the pair here is in BTC-USDT format
	orderType entities.OrderType,
	side entities.OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
) error {
	return cou.txManager.WithinTransaction(ctx, func(ctx context.Context, tx repositories.Tx) error {
		// call the engine to cancel the order -> an event emit
		incomingCancelOrder := engine.NewOrder(userID, pair, engine.OrderType(orderType), engine.OrderSide(side), price, quantity)
		if err := matchEngine.SubmitCancelOrder(eventProducer, *incomingCancelOrder); err != nil {
			zap.S().Errorw("Failed to submit cancel order to engine", "error", err, "userID", userID, "pair", pair, "orderType", orderType, "side", side, "price", price, "quantity", quantity)
			return err
		}
		// @note the UPDATE change status operation should be handled in the event consumer. the UpdateOrderStatus will handle this.
		return nil
	})
}

func (cou *OrderUsecases) UpdateOrderStatus(ctx context.Context, orderID string, filled decimal.Decimal) error {
	return cou.txManager.WithinTransaction(ctx, func(ctx context.Context, tx repositories.Tx) error {
		if err := tx.Orders().UpdateOrderStatus(ctx, orderID, entities.StatusCancelled, filled); err != nil {
			return err
		}
		return nil
	})
}
