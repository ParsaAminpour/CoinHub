package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

var (
	TradeExecutedConsumerGroupID  string = "trade-executed-consumer-v1"
	TradeExecutedConsumerDLQTopic string = "trade-executed-consumer.dlq"
)

const (
	EventTradeExecuted EventType = "TRADE_EXECUTED" // is emitted once per match iteration
)

type TradeEventTopic = func(metadata string) string

var (
	CoinhubTradeExecutedEventTopic TradeEventTopic = func(metadata string) string {
		return "coinhub.trade.executed"
	}
)

var CoinhubTradeEventDispatcher = func(eventType EventType, metadata string) string {
	switch eventType {
	case EventTradeExecuted:
		return CoinhubTradeExecutedEventTopic(metadata)
	default:
		return CoinhubTradeExecutedEventTopic(metadata)
	}
}

var CoinhubAllTradeTopicsByEventTypes = func(eventTypes []EventType, metadata string) []string {
	var topics []string
	for _, e := range eventTypes {
		topics = append(topics, CoinhubTradeEventDispatcher(e, metadata))
	}
	return topics
}

var CoinhubAllCurrentTradeTopics = func() []string {
	return []string{
		CoinhubTradeExecutedEventTopic(""),
	}
}

type TradeEvent interface {
	GetEventHeader() EventHeader
	GetMakerOrderID() string
	GetTakerOrderID() string
	GetPrice() decimal.Decimal
	GetPair() string
	GetQuantity() decimal.Decimal
}

// Maker — the order that was already sitting in the orderbook (resting order). It made liquidity.
// Taker — the incoming order that matches against the resting one. It takes liquidity from the book.
type TradeStatusEvent struct {
	EventHeader
	MakerOrderID   string          `json:"maker_order_id"`
	TakerOrderID   string          `json:"taker_order_id"`
	Pair           string          `json:"pair"`
	Price          decimal.Decimal `json:"price"`
	Quantity       decimal.Decimal `json:"quantity"`
	MakerFilled    bool            `json:"maker_filled"`
	TakerFilled    bool            `json:"taker_filled"`
	MakerRemaining string          `json:"maker_remaining"` // decimal string
	TakerRemaining string          `json:"taker_remaining"` // decimal string
}

func NewTradeStatusEvent(
	makerOrderID string,
	takerOrderID string,
	pair string,
	price decimal.Decimal,
	quantity decimal.Decimal,
	makerFilled bool,
	takerFilled bool,
	makerRemaining decimal.Decimal,
	takerRemaining decimal.Decimal,
) TradeStatusEvent {
	if err := validateNewTradeEventInputs(makerOrderID, takerOrderID, pair, price, quantity); err != nil {
		zap.S().Errorw("invalid NewTradeEvent input", "error", err)
		return TradeStatusEvent{}
	}
	return TradeStatusEvent{
		EventHeader:    EventHeader{EventID: uuid.NewString(), EventType: EventTradeExecuted, Version: "v1", OccuredAt: time.Now()},
		MakerOrderID:   makerOrderID,
		TakerOrderID:   takerOrderID,
		Pair:           pair,
		Price:          price,
		Quantity:       quantity,
		MakerFilled:    makerFilled,
		TakerFilled:    takerFilled,
		MakerRemaining: makerRemaining.String(),
		TakerRemaining: takerRemaining.String(),
	}
}

func validateNewTradeEventInputs(makerOrderID string, takerOrderID string, pair string, price decimal.Decimal, quantity decimal.Decimal) error {
	if makerOrderID == "" {
		return fmt.Errorf("maker order id is required")
	}
	if takerOrderID == "" {
		return fmt.Errorf("taker order id is required")
	}
	if pair == "" {
		return fmt.Errorf("pair is required")
	}
	if !(strings.Contains(pair, "-") || strings.Contains(pair, "/")) {
		return fmt.Errorf("pair should be in format BASE-QUOTE or BASE/QUOTE")
	}
	if price.IsNegative() {
		return fmt.Errorf("price should not be negative")
	}
	if quantity.IsNegative() {
		return fmt.Errorf("quantity should not be negative")
	}
	return nil
}

func (tes TradeStatusEvent) GetEventHeader() EventHeader  { return tes.EventHeader }
func (tes TradeStatusEvent) GetMakerOrderID() string      { return tes.MakerOrderID }
func (tes TradeStatusEvent) GetTakerOrderID() string      { return tes.TakerOrderID }
func (tes TradeStatusEvent) GetPrice() decimal.Decimal    { return tes.Price }
func (tes TradeStatusEvent) GetQuantity() decimal.Decimal { return tes.Quantity }
func (tes TradeStatusEvent) GetPair() string              { return tes.Pair }
