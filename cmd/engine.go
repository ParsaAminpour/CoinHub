package cmd

import (
	"coinhub/internal"
	"coinhub/internal/engine"
	"coinhub/internal/infrastructure/configs"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// Matching enginer of our price-time priority centralized orderbook.

// NOTEs:
// One mutex per orderbook pair — your matching engine must be single-threaded per trading pair (e.g. BTC/USDT gets its own lock). Matching can't be concurrent within a pair or you'll corrupt state.
// Channel-based order queue — orders come in via HTTP, get pushed into a chan Order, and your matching goroutine processes them sequentially. This gives you a natural serialization point.
// sync.RWMutex for reads — the orderbook snapshot endpoint gets read-locked; matching gets write-locked. This lets many readers (WebSocket clients) see the book simultaneously.
// The order of operations on startup matters: load the book first, then open the HTTP server. Never accept new orders before the book is restored
// Pitfall: buying your sell, aka. wash trading! - Check user.ID at the first part of the loop to prevent that!
// Set a minimum order size per pair and enforce it at the API layer before the order ever reaches the engine
// handle dust that accumulates during partial fills — if after a fill the remaining quantity drops below the minimum, treat the order as fully filled rather than leaving a dust remainder in the book
// Consider `ON CONFLICT (id) DO NOTHING` to prevent double mutation for one trades tables, it cause massive issue!
func RunOrderMatchEngine(configs *configs.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "engine",
		Short: "Order Match Engine V1",
		Long:  "Orderbook Match Engine based on Price-Time Priority",
		Run: func(cmd *cobra.Command, args []string) {
			wg := sync.WaitGroup{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// the match engine initialized in here
			app := internal.NewApplication(ctx, configs)

			closeSignal := make(chan os.Signal, 1)
			signal.Notify(closeSignal, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
			defer signal.Stop(closeSignal)

			// The processese that will run in this handler:
			// - We run engine for each pair (each engine has their own orderbook)
			// - Each pair have their own engine and run Match algorithm for each incoming orders comes to order channel - it's prevent Cancel race condition.
			// - The engine will emit an even after it can match two sides to the event streamer (Kafka or just a buferred channel)
			// - The consumer will take the event and update the DB and notify the userID client via socket.
			if err := engine.SetupMatchEngine(ctx, &wg, app.OrderEventProducer, app.AssetRepository, app.OrderRepository, app.OrderMatchEngine); err != nil {
				zap.S().Fatalw("an error occurred in match engine", "error", err)
			}

			select {
			case sig := <-closeSignal:
				zap.S().Infow("terminating by OS signal", "signal", sig.String())
				cancel()
			case <-ctx.Done():
				zap.S().Info("terminating by context cancellation")
			}
			zap.S().Infow("Engine is running...")
			wg.Wait()
			zap.S().Debug("shutdown complete")
		},
	}
	return cmd
}
