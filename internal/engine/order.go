package engine

import (
	"coinhub/internal/domain/entities"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

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

	OrderBehaviorTIF OrderBehavior = "TIF"
	OrderBehaviorGTC OrderBehavior = "GTC"
	OrderBehaviorIOC OrderBehavior = "IOC"
	OrderBehaviorALO OrderBehavior = "ALO"
)

// NOTE : the price and quntitiy section would be decimal.Zero if the order type was cancel.
type Order struct {
	ID        string
	UserID    string
	Pair      string // NOTE : the Order in engine and kafka is in BTC-USDT format, unlikely in DB which is BTC/USDT format.
	Type      OrderType
	Side      OrderSide
	Behavior  OrderBehavior
	Price     decimal.Decimal // zero for market orders
	Quantity  decimal.Decimal // original quantity, it should be immutable
	Filled    decimal.Decimal // how much has been matched so far, it's mutable
	Status    OrderStatus     // The status won't be filled unless the Remaining was zero
	ExpiresAt time.Time       // resting book expiry (default placement + entities.DefaultRestingOrderLifetime)
	Timestamp time.Time
}

func NewOrder(
	userID string,
	pair string,
	orderType OrderType,
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
	expireAt *time.Time,
) *Order {
	now := time.Now()
	return &Order{
		ID:        uuid.NewString(),
		UserID:    userID,
		Pair:      pair,
		Type:      orderType,
		Side:      side,
		Behavior:  OrderBehaviorGTC,
		Price:     price,
		Quantity:  quantity,
		Filled:    decimal.Zero,
		Status:    StatusOpen,
		ExpiresAt: entities.RestingOrderExpiresAt(expireAt, now),
		Timestamp: now,
	}
}

func (o *Order) Remaining() decimal.Decimal {
	return o.Quantity.Sub(o.Filled)
}

func (o *Order) IsPartial() bool {
	return !o.Remaining().IsZero()
}

func (o *Order) IsFilled() bool {
	return o.Remaining().IsZero()
}

func (o *Order) ChangeStatusTo(s OrderStatus) {
	o.Status = s
}

func (o *Order) AddToFilled(qty decimal.Decimal) {
	o.Filled.Add(qty)
	o.ChangeStatusTo(StatusPartial)
}
