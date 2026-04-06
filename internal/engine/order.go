package engine

import (
	"time"

	"github.com/shopspring/decimal"
)

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

type Order struct {
	ID        string
	UserID    string
	Pair      string // "BTC/USDT"
	Type      OrderType
	Side      OrderSide
	Price     decimal.Decimal // zero for market orders
	Quantity  decimal.Decimal // original quantity, it should be immutable
	Filled    decimal.Decimal // how much has been matched so far, it's mutable
	Status    OrderStatus     // The status won't be filled unless the Remaining was zero
	Timestamp time.Time
}

func (o *Order) Remaining() decimal.Decimal {
	return o.Quantity.Sub(o.Filled)
}

func (o *Order) IsFilled() bool {
	return o.Remaining().IsZero()
}

func (o *Order) ChangeStatusTo(s OrderStatus) {
	o.Status = s
}
