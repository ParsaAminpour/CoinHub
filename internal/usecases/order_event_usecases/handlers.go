package order_event_usecases

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type EventDeduper interface {
	MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
}

type ProjectionHandler struct {
	OrderRepository repositories.OrderRepository
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
	// mark event as proceed.

	// route the order based on its pais to the associated engine.
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
		return errors.New("missing required event fields")
	}
	return nil
}
