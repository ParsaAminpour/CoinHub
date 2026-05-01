package engine

import (
	"github.com/google/btree"
	"github.com/shopspring/decimal"
)

type PriceLevel struct {
	PriceLevel decimal.Decimal
	Orders     []*Order // TODO(feature) : using B-Tree instead of slices
}

func NewPriceLevel(orders []*Order, price decimal.Decimal) *PriceLevel {
	return &PriceLevel{
		PriceLevel: price,
		Orders:     orders,
	}
}

type AskLevel struct{ *PriceLevel }

func (a AskLevel) Less(than btree.Item) bool {
	other := than.(AskLevel)
	return a.PriceLevel.PriceLevel.LessThan(other.PriceLevel.PriceLevel)
}

type BidLevel struct{ *PriceLevel }

func (b BidLevel) Less(than btree.Item) bool {
	other := than.(BidLevel)
	return b.PriceLevel.PriceLevel.GreaterThan(other.PriceLevel.PriceLevel)
}

func (s *PriceLevel) RemoveFilledOrderInPriceLevel(side OrderSide, idx int) {
	s.Orders = append(s.Orders[:idx], s.Orders[idx+1:]...)
}

func (pl *PriceLevel) RemoveOrderInPriceLevelBasedOnOrderID(orderID string) error {
	for i, order := range pl.Orders {
		if order.ID == orderID {
			pl.Orders = append(pl.Orders[:i], pl.Orders[i+1:]...)
			return nil
		}
	}
	return ErrOrderNotFoundInPriceLevel
}
