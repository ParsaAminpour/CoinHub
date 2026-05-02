package order_event_usecases

import (
	coinhub_ws "coinhub/internal/adapter/handler/websockets"
	"coinhub/internal/adapter/handler/websockets/notification"
	kafka "coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"encoding/json"
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

func (h *ProjectionHandler) MarkEvent(ctx context.Context, event any) error {
	// The order itself will be created in POST /order/limit HTTP endpoint
	var eventID string
	switch e := event.(type) {
	case kafka.OrderStatusEvent:
		eventID = e.EventID
	case kafka.TradeStatusEvent:
		eventID = e.EventID
	default:
		return fmt.Errorf("unsupported event type %T in MarkEvent", event)
	}

	if eventID == "" {
		return errors.New("missing event_id")
	}

	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, eventID)
	if err != nil || !inserted {
		return err
	}
	return nil
}

func (h *ProjectionHandler) UpdateOrderStatus(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error {
	if err := h.MarkEvent(ctx, event); err != nil {
		return err
	}
	if err := h.OrderRepository.UpdateOrderStatus(ctx, event.ID, entities.OrderStatus(event.Status), event.Filled); err != nil {
		return err
	}
	zap.S().Infow("projection updated", "consumer_name", h.ConsumerName, "event_id", event.EventID, "order_id", event.ID, "status", event.Status, "partition", record.Partition, "offset", record.Offset)
	return nil
}

func (h *ProjectionHandler) HandleIncmingOrder(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error {
	if err := h.MarkEvent(ctx, event); err != nil {
		return err
	}
	return nil
}

func (h *ProjectionHandler) HandleTradeExecutedEvent(ctx context.Context, event kafka.TradeStatusEvent, record *kgo.Record) error {
	if err := h.MarkEvent(ctx, event); err != nil {
		return err
	}
	// record the trade event to the DB as Trade table.
	tradeEntity, err := entities.NewTrade(event.Pair, event.MakerOrderID, event.TakerOrderID, event.Price, event.Quantity, time.Now())
	if err != nil {
		return err
	}
	if err := h.TradeRepository.CreateTrade(ctx, tradeEntity); err != nil {
		return err
	}
	zap.S().Infow("trade event created", "consumer_name", h.ConsumerName, "event_id", event.EventID, "trade_id", tradeEntity.ID, "pair", tradeEntity.Pair, "maker_order_id", tradeEntity.MakerOrderID, "taker_order_id", tradeEntity.TakerOrderID, "price", tradeEntity.Price, "quantity", tradeEntity.Quantity, "executed_at", tradeEntity.ExecutedAt.Format(time.RFC3339), "topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
	return nil
}

// ====== Order Notification Event Use Cases Handlers ======
type NotificationHandler struct {
	Deduper      EventDeduper
	ConsumerName string
}

func (h *NotificationHandler) MarkEvent(ctx context.Context, event any) error {
	// The order itself will be created in POST /order/limit HTTP endpoint
	var eventID string
	switch e := event.(type) {
	case kafka.OrderStatusEvent:
		eventID = e.EventID
	case kafka.TradeStatusEvent:
		eventID = e.EventID
	default:
		return fmt.Errorf("unsupported event type %T in MarkEvent", event)
	}
	if eventID == "" {
		return errors.New("missing event_id")
	}
	inserted, err := h.Deduper.MarkEventProcessed(ctx, h.ConsumerName, eventID)
	if err != nil || !inserted {
		return err
	}
	return nil
}

// called in the notification event consumer handlers
func (h *NotificationHandler) HandleNotificationForOrders(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record, hub *notification.NotificationServer) error {
	if err := h.MarkEvent(ctx, event); err != nil {
		return err
	}
	zap.S().Infow("sending order notification event to ws", "consumer_name", h.ConsumerName, "event_id", event.EventID, "user_id", event.UserID, "order_id", event.ID, "status", event.Status, "pair", event.Pair, "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "event", event)

	// if you found any event, the hub::client::send()
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	websocketMsg := coinhub_ws.NewMessage(coinhub_ws.TypeEvent, string(event.EventType), eventPayload)
	hub.Hub.BroadcastUser(ctx, event.UserID, websocketMsg)
	return nil
}

// called in the notification event consumer handlers
func (h *NotificationHandler) HandleNotificationForTrades(ctx context.Context, event kafka.TradeStatusEvent, record *kgo.Record, hub *notification.NotificationServer) error {
	if err := h.MarkEvent(ctx, event); err != nil {
		return err
	}
	// if you found any event, the hub::client::send()
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	websocketMsg := coinhub_ws.NewMessage(coinhub_ws.TypeEvent, string(event.EventType), eventPayload)
	hub.Hub.BroadcastUser(ctx, event.MakerUserID, websocketMsg)
	hub.Hub.BroadcastUser(ctx, event.TakerUserID, websocketMsg)
	return nil
}
