package engine

import (
	"errors"

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

func (pl *PriceLevel) BestOrderInPriceLevel() (*Order, error) {
	return pl.Orders[len(pl.Orders)], nil
}

func (pl *PriceLevel) RemoveOrderInPriceLevelBasedOnOrderID(orderID string) error {
	for i, order := range pl.Orders {
		if order.ID == orderID {
			pl.Orders = append(pl.Orders[:i], pl.Orders[i+1:]...)
			return nil
		}
	}
	return errors.New("order not found in price level")
}

func (s *PriceLevel) RemoveFilledOrderInPriceLevel(side OrderSide, idx int) {
	s.Orders = append(s.Orders[:idx], s.Orders[idx+1:]...)
}
