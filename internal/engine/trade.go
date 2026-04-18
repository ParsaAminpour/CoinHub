package engine

import (
	"time"

	"github.com/shopspring/decimal"
)

type Trade struct {
	ID          string
	Pair        string
	BuyOrderID  string
	SellOrderID string
	Price       decimal.Decimal // the price at which it matched
	Quantity    decimal.Decimal // how much actually exchanged
	Timestamp   time.Time
}

type TradeEvent struct {
	Trade      Trade
	MakerOrder Order // the resting order
	TakerOrder Order // the aggressor
}

// tradeCh := make(chan TradeEvent, 1000) // buffered — engine never blocks
