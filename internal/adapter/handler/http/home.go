package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/messaging/kafka"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func GetHome(c *gin.Context, app *internal.Application) error {
	// if err := tasks.EnqueueTransferEventTask(
	// 	c,
	// 	app.AsynqClient,
	// 	"0x64ff606c52067dfa870db3b5700ed4c63c10df1ce72c0f1283887aa2b8f52cab",
	// 	"0x1c255DB352E8B3CC16EFd721C61d7b1B5952b2bb",
	// 	"0xc7d1cd54dcb800919614e6b1f073941b57edfc3e",
	// 	"1000000000",
	// 	"0x036CbD53842c5426634e7929541eC2318f3dCF7e",
	// 	38131927,
	// 	false,
	// ); err != nil {
	// 	zap.S().Errorf("failed to enqueue transfer event task: %v", err)
	// 	return err
	// }

	event := kafka.NewOrderEvent(
		uuid.NewString(),
		"mockUserID",
		"BTC-USDT",
		kafka.OrderTypeLimit,
		kafka.StatusFilled,
		kafka.EventOrderFilled,
		kafka.SideBuy,
		decimal.NewFromFloat(30000.0), // price
		decimal.NewFromFloat(1.5),     // quantity
		decimal.NewFromFloat(1.5),     // filled (fully filled)
		decimal.Zero,                  // remaining
	).(kafka.OrderStatusEvent)
	if err := app.OrderEventProducer.PublishOrderEvent(event); err != nil {
		zap.S().Errorw("failed to publish order filled event", "error", err)
		return err
	}
	zap.S().Infow("Order filled event produced",
		"order_id", event.ID,
		"user_id", event.UserID,
		"pair", event.Pair,
		"type", event.Type,
		"side", event.Side,
		"price", event.Price.String(),
		"quantity", event.Quantity.String(),
		"filled", event.Filled.String(),
		"status", event.Status,
		"event_id", event.EventHeader.EventID,
		"event_type", event.EventHeader.EventType,
		"event_version", event.EventHeader.Version,
		"occured_at", event.EventHeader.OccuredAt,
	)

	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})

	return nil
}
