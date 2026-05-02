package kafka

import (
	"coinhub/internal/domain/entities"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// NOTE : The defaul kafka message size is 1MB

const (
	OrderPrjectionConsumerGroupID   string = "order-projection-consumer-v1"
	OrderSubmittedConsumerGroupID   string = "order-submitted-consumer-v1"
	OrderProjectionConsumerDLQTopic string = "order-projection-consumer.dlq"
	OrderSubmittedConsumerDLQTopic  string = "order-submitted-consumer.dlq"
)

type OrderEventTopic func(metadata string) string

var (
	CoinhubOpenOrderEventTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.open"
	}
	CoinHubFilledOrderEventTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.filled"
	}
	CoinhubPartialOrderEventTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.partial"
	}
	CoinHubOrderStatusTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.status"
	}
	// just use in the engine process.
	CoinHubOrderSubmittedTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.submitted"
	}
	CoinHubOrderPlacedTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.placed"
	}
	CoinHubOrderCanceledTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.canceled"
	}
	CoinHubOrderExpiredTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.order.expired"
	}
	CoinHubTradeExecutedTopic OrderEventTopic = func(metadata string) string {
		return "coinhub.trade.executed"
	}

	CoinhubOrderEventDispatcher = func(eventType EventType, metadata string) string {
		switch eventType {
		case EventOrderPartial:
			return CoinhubPartialOrderEventTopic(metadata)
		case EventOrderFilled:
			return CoinHubFilledOrderEventTopic(metadata)
		case EventOrderStatus:
			return CoinHubOrderStatusTopic(metadata)
		case EventOrderSubmitted:
			return CoinHubOrderSubmittedTopic(metadata)
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

	CoinhubAllOrderTopicsByEventTypes = func(eventTypes []EventType, metadata string) []string {
		var topics []string
		for _, e := range eventTypes {
			topics = append(topics, CoinhubOrderEventDispatcher(e, metadata))
		}
		return topics
	}

	CoinhubAllCurrentOrderTopics = func() []string {
		return []string{
			CoinhubOpenOrderEventTopic(""),
			CoinHubFilledOrderEventTopic(""),
			CoinhubPartialOrderEventTopic(""),
			CoinHubOrderStatusTopic(""),
			CoinHubOrderSubmittedTopic(""),
			CoinHubOrderPlacedTopic(""),
			CoinHubOrderCanceledTopic(""),
			CoinHubOrderExpiredTopic(""),
			CoinHubTradeExecutedTopic(""),
		}
	}
)

// EventType identifies what happened
type EventType string

const (
	EventOrderSubmitted EventType = "ORDER_SUBMITTED" // API accepted order for engine processing
	EventOrderFilled    EventType = "ORDER_FILLED"    // when the order get filled (it almost always comes with TRADE_EXECUTED event)
	EventOrderPartial   EventType = "ORDER_PARTIAL_FILLED"
	EventOrderStatus    EventType = "ORDER_STATUS_CHANGED" // do we need this?
	EventOrderCanceled  EventType = "ORDER_CANCELED"       // when order gets canceled
	EventOrderExpired   EventType = "ORDER_EXPIRED"        // when order hits time out in the orderbook (rare scenario)
)

type OrderStatusEvent struct {
	EventHeader

	ID           string          `json:"id"`
	UserID       string          `json:"user_id"`
	Pair         string          `json:"pair"` // "BTC-USDT"
	Type         OrderType       `json:"type"`
	Side         OrderSide       `json:"side"`
	Behavior     OrderBehavior   `json:"behavior"`
	ExpiresAt    time.Time       `json:"expires_at"`
	Price        decimal.Decimal `json:"price"`
	Quantity     decimal.Decimal `json:"quantity"`
	Filled       decimal.Decimal `json:"filled"`
	Status       OrderStatus     `json:"status"`
	RemainingQty decimal.Decimal `json:"remaining_qty"`
}

func validateNewOrderEventInputs(
	id string,
	userID string,
	pair string,
	orderType OrderType,
	newStatus OrderStatus,
	eventType EventType,
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
	filled decimal.Decimal,
	remainingQty decimal.Decimal,
	behavior OrderBehavior,
	expiresAt time.Time,
) error {
	// validate id
	if id == "" {
		return fmt.Errorf("order id is required")
	}
	// validate userID
	if userID == "" {
		return fmt.Errorf("user id is required")
	}
	// validate pair format
	if pair == "" || !strings.Contains(pair, "-") {
		return fmt.Errorf("pair should be in format BASE/QUOTE")
	}
	// validate order type
	if orderType != OrderTypeLimit && orderType != OrderTypeMarket && orderType != OrderTypeCancel {
		return fmt.Errorf("invalid order type: %s", orderType)
	}
	validBehaviors := map[OrderBehavior]struct{}{
		OrderBehaviorTIF: {}, OrderBehaviorGTC: {}, OrderBehaviorIOC: {}, OrderBehaviorALO: {},
	}
	if _, ok := validBehaviors[behavior]; !ok {
		return fmt.Errorf("invalid order behavior: %s", behavior)
	}
	if expiresAt.IsZero() {
		return fmt.Errorf("expires_at is required")
	}
	// validate status
	validStatuses := map[OrderStatus]struct{}{
		StatusOpen: {}, StatusPartial: {}, StatusFilled: {}, StatusCancelled: {}, StatusExpired: {},
	}
	if _, ok := validStatuses[newStatus]; !ok {
		return fmt.Errorf("invalid order status: %s", newStatus)
	}
	// validate event type
	validEventTypes := map[EventType]struct{}{
		EventOrderSubmitted: {}, EventOrderFilled: {}, EventOrderPartial: {}, EventOrderStatus: {},
		EventOrderCanceled: {}, EventOrderExpired: {}, EventTradeExecuted: {},
	}
	if _, ok := validEventTypes[eventType]; !ok {
		return fmt.Errorf("invalid event type: %s", eventType)
	}
	// validate order side
	if side != SideBuy && side != SideSell {
		return fmt.Errorf("invalid order side: %s", side)
	}
	// validate price
	if price.IsNegative() {
		return fmt.Errorf("price should not be negative")
	}
	// validate quantity
	if quantity.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("quantity should be positive")
	}
	// validate filled
	if filled.IsNegative() {
		return fmt.Errorf("filled quantity should not be negative")
	}
	// validate remainingQty
	if remainingQty.IsNegative() {
		return fmt.Errorf("remainingQty should not be negative")
	}
	return nil
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
	behavior OrderBehavior,
	expiresAt time.Time,
) OrderEvent {
	if behavior == "" {
		behavior = OrderBehaviorGTC
	}
	if expiresAt.IsZero() {
		expiresAt = time.Now().UTC().Add(entities.DefaultRestingOrderLifetime)
	}

	if err := validateNewOrderEventInputs(
		id, userID, pair, orderType, newStatus, eventType, side, price, quantity, filled, remainingQty, behavior, expiresAt,
	); err != nil {
		fmt.Errorf(fmt.Sprintf("invalid NewOrderEvent input: %v", err))
	}

	return &OrderStatusEvent{
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
		Behavior:     behavior,
		ExpiresAt:    expiresAt.UTC(),
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
	GetBaseAsset() string
	GetQuoteAsset() string
}

func (ose OrderStatusEvent) GetEventHeader() EventHeader { return ose.EventHeader }
func (ose OrderStatusEvent) GetOrderID() string          { return ose.ID }
func (ose OrderStatusEvent) GetOrderUserID() string      { return ose.UserID }
func (ose OrderStatusEvent) GetSymbol() string           { return ose.Pair }
func (ose OrderStatusEvent) GetBaseAsset() string        { return strings.Split(ose.Pair, "-")[0] } // e.g. BTC
func (ose OrderStatusEvent) GetQuoteAsset() string       { return strings.Split(ose.Pair, "-")[1] } // e.g. USDT

func (ose *OrderStatusEvent) ChangeStatusEvent(newEventType EventType, newOrderStatus OrderStatus) {
	ose.EventHeader.EventType = newEventType
	ose.Status = newOrderStatus
}
func (ose *OrderStatusEvent) UpdateOrderFilled(qty decimal.Decimal) {
	ose.Filled = ose.Filled.Add(qty)
	ose.RemainingQty = ose.RemainingQty.Sub(qty)
}
