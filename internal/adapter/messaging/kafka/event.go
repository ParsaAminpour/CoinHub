package kafka

import "time"

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
type OrderBehavior string

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

	OrderBehaviorTIF OrderBehavior = "TIF"
	OrderBehaviorGTC OrderBehavior = "GTC"
	OrderBehaviorIOC OrderBehavior = "IOC"
	OrderBehaviorALO OrderBehavior = "ALO"
)
