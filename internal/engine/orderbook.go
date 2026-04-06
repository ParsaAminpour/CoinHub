package engine

import (
	"sync"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type PriceLevel struct {
	PriceLevel decimal.Decimal
	Orders     []*Order // TODO : using B-Tree instead of slices
}

func NewPriceLevel(orders []*Order, price decimal.Decimal) *PriceLevel {
	return &PriceLevel{
		PriceLevel: price,
		Orders:     orders,
	}
}

type Side struct {
	// The last order of the slice has the lowest price level
	Levels []*PriceLevel // could be bids or asks
}

func (s Side) Add(order Order, price decimal.Decimal) error {
	if len(s.Levels) == 0 {
		priceLevel := NewPriceLevel([]*Order{&order}, price)
		_ = append(s.Levels, priceLevel)
	}

	if order.Side == SideBuy {
		for idx, orderPl := range s.Levels { // s.Levels are descending start from idx:0
			nextPriceLevel := s.Levels[idx+1]
			if orderPl.PriceLevel.Equal(price) {
				_ = append(orderPl.Orders, &order) // oldest goes to the last

			} else if orderPl.PriceLevel.LessThan(price) && (nextPriceLevel == nil || nextPriceLevel.PriceLevel.GreaterThan(price)) {
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append(s.Levels[:idx+1], append([]*PriceLevel{priceLevel}, s.Levels[idx+1:]...)...)

			} else if orderPl.PriceLevel.LessThan(price) { // new best bid offer
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append([]*PriceLevel{priceLevel}, s.Levels...)
			}
		}
	} else {
		for idx, orderPl := range s.Levels { // the sequence goes ascending start from the idx:0
			nextPriceLevel := s.Levels[idx+1]
			if orderPl.PriceLevel.Equal(price) {
				_ = append(orderPl.Orders, &order) // oldest goes to the last

			} else if orderPl.PriceLevel.GreaterThan(price) && (nextPriceLevel == nil || nextPriceLevel.PriceLevel.LessThan(price)) {
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append(s.Levels[:idx+1], append([]*PriceLevel{priceLevel}, s.Levels[idx+1:]...)...)

			} else if orderPl.PriceLevel.LessThan(price) { // new best ask offer
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append([]*PriceLevel{priceLevel}, s.Levels...)
			}
		}
	}
	return nil
}

// the best price level for the bids is the first index, meaning the buyer wants to buy at highest price level.
// the best price level for the asks is the first index, meaning the seller wants to sell at lowest price level.
func (s *Side) BestPriceLevel() *PriceLevel {
	if len(s.Levels) == 0 {
		return nil
	}
	return s.Levels[0]
}

func (s *Side) RemovePriceLevel(price decimal.Decimal) error {
	// implement it..
	return nil
}

func (s *Side) PopFront() {
	if len(s.Levels) == 0 {
		return
	}
	best := s.Levels[0]
	best.Orders = best.Orders[1:]
	if len(best.Orders) == 0 {
		s.Levels = s.Levels[1:]
	}
}

// Asks → lowest price at index 0 (best ask = cheapest seller)
// Bids → highest price at index 0 (best bid = most generous buyer)
// index 0 id always the best price
type Orderbook struct {
	Pair string          // each orderbook is associated with a Pair
	Dust decimal.Decimal // each Pair has its own Dust amount
	Bids Side            // descending
	Asks Side            // ascending
	mu   sync.RWMutex
}

// NOTE : you can use any Matching algorithm you want based on the data structure you use for the orders
// TODO : Remove the logs after testing
func (ob *Orderbook) MatchLimit(incomingOrder Order) error {
	if incomingOrder.Side == SideBuy {
		// the buyer doesn't want to go upper than incomingOrder.Price, and the best seller doesn't want to go lower than its price
		bestPriceLevel := ob.Asks.BestPriceLevel()
		if bestPriceLevel.PriceLevel.LessThanOrEqual(incomingOrder.Price) {
			// the incoming order matched and the settle accounts are going to start...
			for _, bestOrder := range bestPriceLevel.Orders { // the first firtsrone (oldest one that appended) has more time priority
				if bestOrder.UserID == incomingOrder.UserID { // prevent self-order and money-washing
					continue // go to the next best order
				}
				fillQty := decimal.Min(bestOrder.Remaining(), incomingOrder.Remaining())
				bestOrder.Filled.Add(fillQty)
				incomingOrder.Filled.Add(fillQty)
				if !fillQty.IsZero() {
					bestOrder.ChangeStatusTo(StatusPartial)
					incomingOrder.ChangeStatusTo(StatusPartial)
					zap.S().Infow("Order partially filled",
						"order_id", bestOrder.ID,
						"status", StatusPartial,
						"filled_qty", fillQty.String(),
						"remaining_qty", bestOrder.Remaining().String(),
					)
					zap.S().Infow("Incoming order partially filled",
						"order_id", incomingOrder.ID,
						"status", StatusPartial,
						"filled_qty", fillQty.String(),
						"remaining_qty", incomingOrder.Remaining().String(),
					)
				}

				if bestOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
					// remove that best order from the queue
					ob.Asks.PopFront()
					zap.S().Infow("Best order filled and removed from the queue",
						"order_id", bestOrder.ID,
						"status", StatusFilled,
						"filled_qty", bestOrder.Filled.String(),
						"price", bestOrder.Price.String(),
					)
					// notify that the order gets filled - Emit TradeEvent to the partitions

				} // if it's not, so the incoming order got filled

				if incomingOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
					// don't keep the incoming order in orderbook
					// notify that the order gets filled
				} else { // stay in the orderbook
					if err := ob.Bids.Add(incomingOrder, incomingOrder.Price); err != nil {
						zap.S().Errorw("Failed to add incoming order to bids", "order_id", incomingOrder.ID, "price", incomingOrder.Price)
					}
					zap.S().Infow("Added incoming order to bids",
						"order_id", incomingOrder.ID,
						"price", incomingOrder.Price.String(),
					)
				}
			}
		}
	} else {
		bestPriceLevel := ob.Bids.BestPriceLevel()
		if bestPriceLevel.PriceLevel.GreaterThanOrEqual(incomingOrder.Price) {
			for _, bestOrder := range bestPriceLevel.Orders {
				fillQty := decimal.Min(bestOrder.Price, incomingOrder.Price)
				bestOrder.Filled.Add(fillQty)
				incomingOrder.Filled.Add(fillQty)
				if !fillQty.IsZero() {
					bestOrder.ChangeStatusTo(StatusPartial)
					incomingOrder.ChangeStatusTo(StatusPartial)
					zap.S().Infow("Order partially filled",
						"order_id", bestOrder.ID,
						"status", StatusPartial,
						"filled_qty", fillQty.String(),
						"remaining_qty", bestOrder.Remaining().String(),
					)
					zap.S().Infow("Incoming order partially filled",
						"order_id", incomingOrder.ID,
						"status", StatusPartial,
						"filled_qty", fillQty.String(),
						"remaining_qty", incomingOrder.Remaining().String(),
					)
				}

				if bestOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
					// remove that best order from the queue
					ob.Bids.PopFront()
					zap.S().Infow("Best order filled and removed from the queue",
						"order_id", bestOrder.ID,
						"status", StatusFilled,
						"filled_qty", bestOrder.Filled.String(),
						"price", bestOrder.Price.String(),
					)
					// notify that the order gets filled - Emit TradeEvent to the partitions

				} // if it's not, so the incoming order got filled

				if incomingOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
					// don't keep the incoming order in orderbook
					// notify that the order gets filled
				} else { // stay in the orderbook
					if err := ob.Asks.Add(incomingOrder, incomingOrder.Price); err != nil {
						zap.S().Errorw("Failed to add incoming order to asks", "order_id", incomingOrder.ID, "price", incomingOrder.Price)
					}
					zap.S().Infow("Added incoming order to asks",
						"order_id", incomingOrder.ID,
						"price", incomingOrder.Price.String(),
					)
				}
			}
		}
	}
	return nil
}

// TODO : implement it
func (ob *Orderbook) MatchMarket(o Order) error

// TODO : implement it
func (ob *Orderbook) Cancel(o Order) error
