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
	Levels *btree.BTreeG[*PriceLevel] // could be bids or asks
	isAsk  bool                       // Control the sort direction
	Degree int                        // sweet spot for cache performance - branching factor.
}

func NewSide(degree int, isAsk bool) Side {
	// true if within that ordering, 'a' < 'b'.
	less := func(a, b *PriceLevel) bool {
		if isAsk {
			return a.PriceLevel.LessThan(b.PriceLevel) // asks: ascending
		}
		return a.PriceLevel.GreaterThan(b.PriceLevel) // bids: discending
	}
	return Side{
		Levels: btree.NewG[*PriceLevel](degree, less),
		isAsk:  isAsk,
		Degree: degree,
	}
}

func (s *Side) Add(order *Order) {
	key := &PriceLevel{PriceLevel: order.Price}
	existing, ok := s.Levels.Get(key)
	if !ok {
		newPriceLevel := &PriceLevel{PriceLevel: order.Price, Orders: make([]*Order, 0)}
		s.Levels.ReplaceOrInsert(newPriceLevel)
	}
	existing.Orders = append(existing.Orders, order)
}

// the best price level for the bids is the first index, meaning the buyer wants to buy at highest price level.
// the best price level for the asks is the first index, meaning the seller wants to sell at lowest price level.
func (s *Side) BestPriceLevel() (*PriceLevel, bool) {
	var best *PriceLevel
	s.Levels.Ascend(func(p1 *PriceLevel) bool {
		best = p1
		return false
	})
	return best, best != nil
}

func (s *Side) PopFront() {

}

// RemoveLevel deletes an empty price level from the tree.
func (s *Side) RemoveLevel(price decimal.Decimal) {
	s.Levels.Delete(&PriceLevel{PriceLevel: price})
}

// GetLevel retrieves a level by exact price (O(log n)).
func (s *Side) GetLevel(price decimal.Decimal) (*PriceLevel, bool) {
	return s.Levels.Get(&PriceLevel{PriceLevel: price})
}

func (s *Side) RemoveEmptyLevel() {
	s.Levels.Ascend(func(p *PriceLevel) bool {
		if len(p.Orders) == 0 {
			s.RemoveLevel(p.PriceLevel)
		}
		return false
	})
}
