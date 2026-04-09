package kafka

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// NOTE : The defaul kafka message size is 1MB

const (
	OrderPrjectionConsumerGroupID string = "order-projection-consumer-v1"
)

type OrderEventTopic func(metadata string) string

var (
	CoinHubFilledOrderEventTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.filled.%s", metadata) // e.g. coinhub.order.filled.BTC/USDT
	}
	CoinhubPartialOrderEventTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.partial.%s", metadata)
	}
	CoinHubOrderStatusTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.status.%s", metadata)
	}
	CoinHubOrderPlacedTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.placed.%s", metadata)
	}
	CoinHubOrderCanceledTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.canceled.%s", metadata)
	}
	CoinHubOrderExpiredTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.expired.%s", metadata)
	}
	CoinHubTradeExecutedTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.trade.executed.%s", metadata)
	}

	CoinhubEventDispatcher = func(eventType EventType, metadata string) string {
		switch eventType {
		case EventOrderPartial:
			return CoinhubPartialOrderEventTopic(metadata)
		case EventOrderFilled:
			return CoinHubFilledOrderEventTopic(metadata)
		case EventOrderStatus:
			return CoinHubOrderStatusTopic(metadata)
		case EventOrderPlaced:
			return CoinHubOrderPlacedTopic(metadata)
		case EventOrderCanceled:
			return CoinHubOrderCanceledTopic(metadata)
		case EventOrderExpired:
			return CoinHubOrderExpiredTopic(metadata)
		case EventTradeExecuted:
			return CoinHubTradeExecutedTopic(metadata)
		default:
			return CoinHubOrderStatusTopic(metadata)
		}
	}
)

// EventType identifies what happened
type EventType string

const (
	EventOrderPlaced   EventType = "ORDER_PLACED" // open
	EventOrderFilled   EventType = "ORDER_FILLED"
	EventOrderPartial  EventType = "ORDER_PARTIAL_FILLED"
	EventOrderStatus   EventType = "ORDER_STATUS_CHANGED" // do we need this?
	EventOrderCanceled EventType = "ORDER_CANCELED"
	EventOrderExpired  EventType = "ORDER_EXPIRED"
	EventTradeExecuted EventType = "TRADE_EXECUTED" // is emitted once per match iteration
)

// EventHeader is embedded in every event — routing + tracing metadata
type EventHeader struct {
	EventID   string    `json:"event_id"` // uuid, unique per event emission
	EventType EventType `json:"event_type"`
	Version   string    `json:"version"`
	OccuredAt time.Time `json:"occured_at"` // when the match happened, not when Kafka received it
}

type OrderType string
type OrderSide string
type OrderStatus string

const (
	OrderTypeLimit  OrderType = "limit"
	OrderTypeMarket OrderType = "market"
	OrderTypeCancel OrderType = "cancel"

	SideBuy  OrderSide = "buy"
	SideSell OrderSide = "sell"

	StatusOpen      OrderStatus = "open"
	StatusPartial   OrderStatus = "partial"
	StatusFilled    OrderStatus = "filled"
	StatusCancelled OrderStatus = "cancelled"
	StatusExpired   OrderStatus = "expired" // TODO : determine the situation that we go into this status.
)

type OrderStatusEvent struct {
	EventHeader

	ID           string          `json:"id"`
	UserID       string          `json:"user_id"`
	Pair         string          `json:"pair"` // "BTC/USDT"
	Type         OrderType       `json:"type"`
	Side         OrderSide       `json:"side"`
	Price        decimal.Decimal `json:"price"`
	Quantity     decimal.Decimal `json:"quantity"`
	Filled       decimal.Decimal `json:"filled"`
	Status       OrderStatus     `json:"status"`
	RemainingQty decimal.Decimal `json:"remaining_qty"`
}

func NewOrderEvent(
	id string,
	userID string,
	pair string,
	orderType OrderType,
	newStatus OrderStatus, // related to the DB
	eventType EventType, // related to the event
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
	filled decimal.Decimal,
	remainingQty decimal.Decimal,
) OrderEvent {
	return OrderStatusEvent{
		EventHeader: EventHeader{
			EventID:   uuid.NewString(),
			EventType: eventType,
			Version:   "v1",
			OccuredAt: time.Now(),
		},
		ID:           id,
		UserID:       userID,
		Pair:         pair,
		Type:         orderType,
		Side:         side,
		Price:        price,
		Quantity:     quantity,
		Filled:       filled,
		RemainingQty: remainingQty,
		Status:       newStatus,
	}
}

type OrderEvent interface {
	ChangeStatusEvent(newEventType EventType, newOrderStatus OrderStatus)
	UpdateOrderFilled(qty decimal.Decimal)
	GetEventHeader() EventHeader
	GetOrderID() string
	GetOrderUserID() string
	GetSymbol() string
}

func (ose OrderStatusEvent) GetEventHeader() EventHeader { return ose.EventHeader }
func (ose OrderStatusEvent) GetOrderID() string          { return ose.ID }
func (ose OrderStatusEvent) GetOrderUserID() string      { return ose.UserID }
func (ose OrderStatusEvent) GetSymbol() string           { return ose.Pair }
func (ose OrderStatusEvent) ChangeStatusEvent(newEventType EventType, newOrderStatus OrderStatus) {
	ose.EventHeader.EventType = newEventType
	ose.Status = newOrderStatus
}
func (ose OrderStatusEvent) UpdateOrderFilled(qty decimal.Decimal) {
	ose.Filled.Add(qty)
	ose.RemainingQty.Sub(qty)
}
