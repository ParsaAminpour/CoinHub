package cmd

import (
	"context"

	"github.com/spf13/cobra"
)

// Matching enginer of our price-time priority centralized orderbook.

// NOTEs:
// One mutex per orderbook pair — your matching engine must be single-threaded per trading pair (e.g. BTC/USDT gets its own lock). Matching can't be concurrent within a pair or you'll corrupt state.
// Channel-based order queue — orders come in via HTTP, get pushed into a chan Order, and your matching goroutine processes them sequentially. This gives you a natural serialization point.
// sync.RWMutex for reads — the orderbook snapshot endpoint gets read-locked; matching gets write-locked. This lets many readers (WebSocket clients) see the book simultaneously.

func RunMatchingEngine(ctx context.Context) *cobra.Command {

	return nil
}
