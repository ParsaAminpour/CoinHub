package order_event_usecases

import (
	kafka "coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type EventDeduper interface {
	MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
}

type ProjectionHandler struct {
	OrderRepository repositories.OrderRepository
	TradeRepository repositories.TradeRepository
	Deduper         EventDeduper
	ConsumerName    string
}

func (h *ProjectionHandler) Handle(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error {
	if event.EventID == "" {
		return errors.New("missing event_id")
	}

	// the order itself will create in POST /order/limit HTTP endpoint
	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, event.EventID)
	if err != nil {
		return err
	}
	if !inserted {
		zap.S().Infow("duplicate event ignored",
			"consumer_name", h.ConsumerName,
			"event_id", event.EventID,
			"order_id", event.ID,
			"topic", record.Topic,
		)
		return nil
	}
	if err := h.OrderRepository.UpdateOrderStatus(ctx, event.ID, entities.OrderStatus(event.Status), event.Filled); err != nil {
		return err
	}
	zap.S().Infow("projection updated",
		"consumer_name", h.ConsumerName,
		"event_id", event.EventID,
		"order_id", event.ID,
		"status", event.Status,
		"partition", record.Partition,
		"offset", record.Offset,
	)
	return nil
}

func (h *ProjectionHandler) HandleIncmingOrder(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error {
	if event.EventID == "" {
		return errors.New("missing event_id")
	}

	// the order itself will create in POST /order/limit HTTP endpoint
	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, event.EventID)
	if err != nil {
		return err
	}
	if !inserted {
		zap.S().Infow("duplicate event ignored",
			"consumer_name", h.ConsumerName,
			"event_id", event.EventID,
			"order_id", event.ID,
			"topic", record.Topic,
		)
		return nil
	}
	return nil
}

func (h *ProjectionHandler) HandleTradeExecutedEvent(ctx context.Context, event kafka.TradeStatusEvent, record *kgo.Record) error {
	if event.EventID == "" {
		return errors.New("missing event_id")
	}

	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, event.EventID)
	if err != nil {
		return err
	}
	if !inserted {
		zap.S().Infow("duplicate event ignored",
			"consumer_name", h.ConsumerName,
			"event_id", event.EventID,
			"maker_order_id", event.MakerOrderID,
			"taker_order_id", event.TakerOrderID,
			"pair", event.Pair,
			"price", event.Price,
			"quantity", event.Quantity,
			"topic", record.Topic,
		)
		return nil
	}

	// record the trade event to the DB as Trade table.
	tradeEntity, err := entities.NewTrade(event.Pair, event.MakerOrderID, event.TakerOrderID, event.Price, event.Quantity, time.Now())
	if err != nil {
		return err
	}
	if err := h.TradeRepository.CreateTrade(ctx, tradeEntity); err != nil {
		return err
	}

	zap.S().Infow("trade event created",
		"consumer_name", h.ConsumerName,
		"event_id", event.EventID,
		"trade_id", tradeEntity.ID,
		"pair", tradeEntity.Pair,
		"maker_order_id", tradeEntity.MakerOrderID,
		"taker_order_id", tradeEntity.TakerOrderID,
		"price", tradeEntity.Price,
		"quantity", tradeEntity.Quantity,
		"executed_at", tradeEntity.ExecutedAt.Format(time.RFC3339),
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
	)
	return nil
}

// ====== Order Notification Event Use Cases Handlers ======
type NotificationHandler struct {
	Deduper      EventDeduper
	ConsumerName string
}

// for notification handler.
func (h *NotificationHandler) Handle(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error {
	if event.EventID == "" {
		return errors.New("missing event_id")
	}
	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, event.EventID)
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}

	// MVP notifier: this is where websocket fan-out should happen.
	zap.S().Infow("notification event consumed",
		"consumer_name", h.ConsumerName,
		"event_id", event.EventID,
		"user_id", event.UserID,
		"order_id", event.ID,
		"status", event.Status,
		"topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
	)
	return nil
}

func ValidateStatusEvent(event kafka.OrderStatusEvent) error {
	if event.EventHeader.Version != "v1" {
		return fmt.Errorf("unsupported event version: %s", event.EventHeader.Version)
	}
	if event.ID == "" || event.UserID == "" || event.Pair == "" {
		err := errors.New("missing required event fields")
		zap.S().Errorw("missing required event fields",
			"error", err,
			"event_id", event.ID,
			"user_id", event.UserID,
			"pair", event.Pair,
			"event_header_version", event.EventHeader.Version,
		)
		return errors.New("missing required event fields")
	}
	return nil
}
