package engine

import (
	"container/heap"
	"time"

	"github.com/shopspring/decimal"
)

// the heap sorts by expiresAt ascending - smallest (soonest) at the top
type expiryEntity struct {
	expiresAt time.Time
	pair      string          // which orderbook channel to inject into
	orderID   string          // for lazy-deletion check
	price     decimal.Decimal // needed to locate the order in the book
	side      OrderSide       // bids or asks
	index     int             // required by container/heap
}

func NewExpiryEntity(expiresAt time.Time, pair string, orderID string, price decimal.Decimal, side OrderSide, index int) *expiryEntity {
	return &expiryEntity{
		expiresAt: expiresAt,
		pair:      pair,
		orderID:   orderID,
		price:     price,
		side:      side,
		index:     index,
	}
}

func (e expiryEntity) isExpired() bool {
	return e.expiresAt.Before(time.Now())
}

type BookReaper []*expiryEntity

func (br BookReaper) Len() int { return len(br) }

// Less makes this a min-heap: soonest expiresAt at index 0 (heap root).
func (br BookReaper) Less(i, j int) bool {
	return br[i].expiresAt.Before(br[j].expiresAt)
}

func (br BookReaper) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
	br[i].index = i
	br[j].index = j
}

// Push adds x to the heap; required by container/heap. Call heap.Push(br, item).
func (br *BookReaper) Push(x interface{}) {
	heap.Push(br, x)
}

// Pop removes and returns the minimum (root after heap.Fix); required by container/heap.
func (br *BookReaper) Pop() interface{} {
	return heap.Pop(br)
}
