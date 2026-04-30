package engine

import (
	"github.com/google/btree"
	"github.com/shopspring/decimal"
)

// NOTE : Degree t controls the minimum, not maximum, with degree := t we have (t - 1) as min keys per node and (2t - 1) as max keys per node
//
//	and the min children of (t) and the max children of (2t).
//
// So degree=32 means each node holds between 31 and 63 price levels.
// A split only happens when a node overflows past 63. For an order book with, say, 200 active price levels, your tree is almost certainly a single root node
// all 200 levels fit within one node since
// NOTE : The "sweet spot" comment in below refers to CPU cache lines — a node with ~32–64 keys fits well in L1/L2 cache, so scanning keys within a node is fast sequential memory access, not random pointer chasing.
type Side struct {
	// The last order of the slice has the lowest price level
	Levels []*PriceLevel // could be bids or asks
	Lvls   []*btree.BTreeG[*PriceLevel]
	isAsk  bool // Control the sort direction
	Degree int  // sweet spot for cache performance - branching factor.
}

func NewSide(degree int, isAsk bool) Side {
	// TODO : implement here
	return Side{}
}

func (s *Side) Add(order Order, price decimal.Decimal) error {
	// If an existing level matches the price, append the order there (FIFO within level).
	for _, lvl := range s.Levels {
		if lvl.PriceLevel.Equal(price) {
			lvl.Orders = append(lvl.Orders, &order)
			return nil
		}
	}

	// New price level — insert at the correct sorted position.
	// Bids: descending (highest price first).
	// Asks: ascending (lowest price first).
	newLevel := NewPriceLevel([]*Order{&order}, price)
	inserted := false
	newLevels := make([]*PriceLevel, 0, len(s.Levels)+1)
	for _, lvl := range s.Levels {
		if !inserted {
			isBefore := (order.Side == SideBuy && price.GreaterThan(lvl.PriceLevel)) ||
				(order.Side == SideSell && price.LessThan(lvl.PriceLevel))
			if isBefore {
				newLevels = append(newLevels, newLevel)
				inserted = true
			}
		}
		newLevels = append(newLevels, lvl)
	}
	if !inserted {
		newLevels = append(newLevels, newLevel)
	}
	s.Levels = newLevels
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
