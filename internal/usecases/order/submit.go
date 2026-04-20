package order_usercases

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/engine"
	"context"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// it creates an open order in DB and add an submitted event to the Broker.
func (ou *OrderUsecases) SubmitOrder(
	ctx context.Context,
	matchEngine *engine.MatchEngine,
	eventProducer *kafka.EngineEventProducer,
	userID string,
	pair string, // the pair here is in BTC-USDT format
	orderType entities.OrderType,
	side entities.OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal) error {
	// register it on DB
	return ou.txManager.WithinTransaction(ctx, func(ctx context.Context, tx repositories.Tx) error {
		var formattedPairForDB string
		if strings.Contains(pair, "/") || strings.Contains(pair, "-") {
			formattedPairForDB = fmt.Sprintf("%s/%s", strings.Split(pair, "-")[0], strings.Split(pair, "-")[1])
		} else {
			return fmt.Errorf("pair has wrong format neither 'X-Y' nor 'X/Y' format")
		}

		orderEntity := entities.NewOrder(userID, formattedPairForDB, orderType, side, price, quantity)
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
