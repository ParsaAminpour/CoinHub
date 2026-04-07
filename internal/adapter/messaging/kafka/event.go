package kafka

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// NOTE : The defaul kafka message size is 1MB

const (
	ConsumerGroup string = "OrderEvent"
)

type OrderEventTopic func(metadata string) string

var (
	CoinHubFilledOrderEventTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.filled.%s", metadata) // e.g. coinhub.order.filled.BTC/USDT
	}
	CoinhubPartialOrderEventTopic OrderEventTopic = func(metadata string) string {
		return fmt.Sprintf("coinhub.order.partial.%s", metadata)
	}
)

// EventType identifies what happened
type EventType string

const (
	EventOrderFilled        EventType = "ORDER_FILLED"
	EventOrderPartialFilled EventType = "ORDER_PARTIAL_FILLED"
)

// EventHeader is embedded in every event — routing + tracing metadata
type EventHeader struct {
	EventID   string    `json:"event_id"` // uuid, unique per event emission
	EventType EventType `json:"event_type"`
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
)

type OrderFilledEvent struct {
	EventHeader

	ID       string
	UserID   string
	Pair     string // "BTC/USDT"
	Type     OrderType
	Side     OrderSide
	Price    decimal.Decimal
	Quantity decimal.Decimal
	Filled   decimal.Decimal
}

type OrderPartialEvent struct {
	EventHeader

	ID           string
	UserID       string
	Pair         string // "BTC/USDT"
	Type         OrderType
	Side         OrderSide
	Price        decimal.Decimal
	Quantity     decimal.Decimal
	Filled       decimal.Decimal
	RemainingQty decimal.Decimal
}

type OrderEvent interface {
	getEventHeader() EventHeader
	getOrderID() string
	getOrderUserID() string
	getSymbol() string
}

func (ofe OrderFilledEvent) getEventHeader() EventHeader { return ofe.EventHeader }
func (ofe OrderFilledEvent) getOrderID() string          { return ofe.ID }
func (ofe OrderFilledEvent) getOrderUserID() string      { return ofe.UserID }
func (ofe OrderFilledEvent) getSymbol() string           { return ofe.Pair }

func (ope OrderPartialEvent) getEventHeader() EventHeader { return ope.EventHeader }
func (ope OrderPartialEvent) getORderID() string          { return ope.ID }
func (ope OrderPartialEvent) getOrderUserID() string      { return ope.UserID }
func (ope OrderPartialEvent) getSymbol() string           { return ope.Pair }
