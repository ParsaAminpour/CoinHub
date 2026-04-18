package tests

import (
	"sync"
	"testing"

	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/engine"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// Mock event publisher
// ─────────────────────────────────────────────

// capturePublisher satisfies kafka.EventPublisher and records every event
// that MatchLimit would normally send to Kafka.
type capturePublisher struct {
	mu     sync.Mutex
	events []kafka.OrderEvent
}

func (p *capturePublisher) PublishOrderEvent(event kafka.OrderEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	zap.S().Infow("[publisher] PublishOrderEvent",
		"order_id", event.GetOrderID(),
		"user_id", event.GetOrderUserID(),
		"event_type", event.GetEventHeader().EventType,
		"symbol", event.GetSymbol(),
	)
	p.events = append(p.events, event)
	return nil
}

func (p *capturePublisher) PublishOrderEventBatch(events []kafka.OrderEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		zap.S().Infow("[publisher] PublishOrderEventBatch — single event",
			"order_id", e.GetOrderID(),
			"user_id", e.GetOrderUserID(),
			"event_type", e.GetEventHeader().EventType,
			"symbol", e.GetSymbol(),
		)
	}
	p.events = append(p.events, events...)
	return nil
}

func (p *capturePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *capturePublisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
}

// ─────────────────────────────────────────────
// Logger setup
// ─────────────────────────────────────────────

// initLogger installs a development zap logger as the global sugared logger so
// that every zap.S() call inside MatchLimit is printed to stdout during tests.
func initLogger(t *testing.T) {
	t.Helper()
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("failed to build zap logger: %v", err)
	}
	zap.ReplaceGlobals(logger)
	t.Cleanup(func() { _ = logger.Sync() })
}

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func d(f float64) decimal.Decimal { return decimal.NewFromFloat(f) }

// newLimitOrder creates a limit order with an explicit ID so Kafka event
// validation (which rejects empty IDs) doesn't silently fail.
func newLimitOrder(id, userID, pair string, side engine.OrderSide, price, qty float64) *engine.Order {
	o := engine.NewOrder(userID, pair, engine.OrderTypeLimit, side, d(price), d(qty))
	o.ID = id
	return o
}

// newOrderbook creates an Orderbook for the given pair with a small dust
// threshold so near-zero remainders are treated as filled.
func newOrderbook(pair string) *engine.Orderbook {
	return &engine.Orderbook{
		Pair: pair,
		Dust: d(0.0001),
	}
}

// seedAsks directly populates the ask side of the book with one price level.
// Asks are ascending — index 0 is the best (lowest) ask.
func seedAsks(ob *engine.Orderbook, price float64, orders ...*engine.Order) {
	level := &engine.PriceLevel{
		PriceLevel: d(price),
		Orders:     orders,
	}
	ob.Asks.Levels = append([]*engine.PriceLevel{level}, ob.Asks.Levels...)
}

// seedBids directly populates the bid side with one price level.
// Bids are descending — index 0 is the best (highest) bid.
func seedBids(ob *engine.Orderbook, price float64, orders ...*engine.Order) {
	level := &engine.PriceLevel{
		PriceLevel: d(price),
		Orders:     orders,
	}
	ob.Bids.Levels = append([]*engine.PriceLevel{level}, ob.Bids.Levels...)
}

func logBookState(t *testing.T, ob *engine.Orderbook) {
	t.Helper()
	zap.S().Infow("[book state]",
		"pair", ob.Pair,
		"ask_levels", len(ob.Asks.Levels),
		"bid_levels", len(ob.Bids.Levels),
	)
	for i, lvl := range ob.Asks.Levels {
		zap.S().Infow("[ask level]", "idx", i, "price", lvl.PriceLevel.String(), "order_count", len(lvl.Orders))
		for j, o := range lvl.Orders {
			zap.S().Infow("[ask order]", "level_idx", i, "order_idx", j,
				"id", o.ID, "qty", o.Quantity.String(), "filled", o.Filled.String(), "remaining", o.Remaining().String())
		}
	}
	for i, lvl := range ob.Bids.Levels {
		zap.S().Infow("[bid level]", "idx", i, "price", lvl.PriceLevel.String(), "order_count", len(lvl.Orders))
		for j, o := range lvl.Orders {
			zap.S().Infow("[bid order]", "level_idx", i, "order_idx", j,
				"id", o.ID, "qty", o.Quantity.String(), "filled", o.Filled.String(), "remaining", o.Remaining().String())
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — Buy side (incoming = BUY limit)
// ─────────────────────────────────────────────────────────────────────────────

// TestMatchLimit_Buy_EmptyAsks: no resting asks → MatchLimit must return ErrOrderbookEmpty immediately.
func TestMatchLimit_Buy_EmptyAsks(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 1)

	zap.S().Infow("=== TestMatchLimit_Buy_EmptyAsks: setup ===",
		"incoming_id", incoming.ID,
		"incoming_price", incoming.Price.String(),
		"incoming_qty", incoming.Quantity.String(),
		"ask_levels_before", len(ob.Asks.Levels),
	)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_EmptyAsks: result ===",
		"error", err,
		"events_published", pub.count(),
	)

	if err != engine.ErrOrderbookEmpty {
		t.Errorf("expected ErrOrderbookEmpty, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events published, got %d", pub.count())
	}
}

// TestMatchLimit_Sell_EmptyBids: no resting bids → ErrOrderbookEmpty.
func TestMatchLimit_Sell_EmptyBids(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	incoming := newLimitOrder("sell-001", "user-B", "BTC-USDT", engine.SideSell, 49_900, 1)

	zap.S().Infow("=== TestMatchLimit_Sell_EmptyBids: setup ===",
		"incoming_id", incoming.ID,
		"incoming_price", incoming.Price.String(),
		"bid_levels_before", len(ob.Bids.Levels),
	)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_EmptyBids: result ===", "error", err, "events", pub.count())

	if err != engine.ErrOrderbookEmpty {
		t.Errorf("expected ErrOrderbookEmpty, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events published, got %d", pub.count())
	}
}

// TestMatchLimit_Buy_PriceNoMatch: best ask (50 200) is above the incoming buy price (50 100).
// Expectation: no match, no events, ask side unchanged.
func TestMatchLimit_Buy_PriceNoMatch(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_200, 1)
	seedAsks(ob, 50_200, resting)

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 1)

	zap.S().Infow("=== TestMatchLimit_Buy_PriceNoMatch: setup ===",
		"best_ask_price", 50_200, "incoming_buy_price", 50_100)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PriceNoMatch: result ===",
		"error", err, "events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels))

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events (no match), got %d", pub.count())
	}
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("ask side should be untouched, got %d levels", len(ob.Asks.Levels))
	}
}

// TestMatchLimit_Sell_PriceNoMatch: best bid (49 800) is below the incoming sell price (49 900).
// Expectation: order is cancelled, one cancel event is published.
func TestMatchLimit_Sell_PriceNoMatch(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 49_800, 1)
	seedBids(ob, 49_800, resting)

	incoming := newLimitOrder("sell-001", "user-B", "BTC-USDT", engine.SideSell, 49_900, 1)

	zap.S().Infow("=== TestMatchLimit_Sell_PriceNoMatch: setup ===",
		"best_bid_price", 49_800, "incoming_sell_price", 49_900)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_PriceNoMatch: result ===",
		"error", err, "events_published", pub.count())

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// A cancel event must be published for the incoming order.
	if pub.count() != 1 {
		t.Errorf("expected 1 cancel event, got %d", pub.count())
	}
}

// TestMatchLimit_Buy_SelfTrade: incoming buyer and resting seller have the same userID.
// Expectation: order is skipped entirely — no fill, no events published for that pair.
func TestMatchLimit_Buy_SelfTrade(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	sameUser := "user-A"
	resting := newLimitOrder("ask-001", sameUser, "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newLimitOrder("buy-001", sameUser, "BTC-USDT", engine.SideBuy, 50_100, 1)

	zap.S().Infow("=== TestMatchLimit_Buy_SelfTrade: setup ===",
		"user_id", sameUser,
		"resting_ask_price", resting.Price.String(),
		"incoming_buy_price", incoming.Price.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_SelfTrade: result ===",
		"error", err, "events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Self-trade prevention: the resting order was skipped, so nothing should change.
	if pub.count() != 0 {
		t.Errorf("self-trade: expected 0 events published, got %d", pub.count())
	}
	if len(ob.Asks.Levels) != 1 || len(ob.Asks.Levels[0].Orders) != 1 {
		t.Errorf("self-trade: ask side must be untouched, got %d levels", len(ob.Asks.Levels))
	}
}

// TestMatchLimit_Buy_FullFill: incoming buy qty == resting ask qty (both = 1 BTC).
// Price: best ask = 50 000, incoming limit = 50 100 → price matches.
//
// Expected after match:
//   - Both orders are fully filled.
//   - The resting ask is removed from the book (Asks.Levels empty).
//   - The incoming order is NOT added to the bid side.
//   - 2 events published (one per order), both with EventOrderFilled.
func TestMatchLimit_Buy_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 1)

	zap.S().Infow("=== TestMatchLimit_Buy_FullFill: setup ===",
		"resting_ask_id", resting.ID,
		"resting_ask_price", resting.Price.String(),
		"resting_ask_qty", resting.Quantity.String(),
		"incoming_buy_id", incoming.ID,
		"incoming_buy_price", incoming.Price.String(),
		"incoming_buy_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_FullFill: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Both orders are fully matched → 2 events must be published.
	if pub.count() != 2 {
		t.Errorf("expected 2 events (one per order), got %d", pub.count())
	}
	// Resting ask must be removed from the book.
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("resting ask should be removed after full fill, ask levels = %d", len(ob.Asks.Levels))
	}
	// Incoming buy is fully filled → must NOT be added to the bid side.
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("incoming order should NOT be added to bids after full fill, bid levels = %d", len(ob.Bids.Levels))
	}
}

// TestMatchLimit_Buy_PartialFill_IncomingLarger: incoming buy qty (2 BTC) > resting ask qty (1 BTC).
//
// Expected after match:
//   - Resting ask is fully filled and removed from the book.
//   - Incoming order is partially filled (1 BTC remaining) and added to the bid side.
//   - 2 events published.
func TestMatchLimit_Buy_PartialFill_IncomingLarger(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 2)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingLarger: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingLarger: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask (qty=1) is fully consumed → ask side must be empty.
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("resting ask should be removed, ask levels = %d", len(ob.Asks.Levels))
	}
	// Incoming still has 1 BTC remaining → must be resting on the bid side.
	if len(ob.Bids.Levels) != 1 {
		t.Errorf("incoming partial order should be added to bids, bid levels = %d", len(ob.Bids.Levels))
	}
}

// TestMatchLimit_Buy_PartialFill_IncomingSmaller: incoming buy qty (0.5 BTC) < resting ask qty (1 BTC).
//
// Expected after match:
//   - Incoming buy is fully filled (removed from the pipeline).
//   - Resting ask is partially filled (0.5 BTC remaining) and stays in the book.
//   - 2 events published.
func TestMatchLimit_Buy_PartialFill_IncomingSmaller(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 0.5)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingSmaller: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingSmaller: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask is only partially consumed → must still be in the book.
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("resting ask should remain in book after partial fill, ask levels = %d", len(ob.Asks.Levels))
	}
	restingAfter := ob.Asks.Levels[0].Orders[0]
	expectedRemaining := d(0.5)
	if !restingAfter.Remaining().Equal(expectedRemaining) {
		t.Errorf("resting ask remaining should be 0.5, got %s", restingAfter.Remaining().String())
	}
	// Incoming is fully filled → must NOT be added to bids.
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("fully-filled incoming should not be added to bids, bid levels = %d", len(ob.Bids.Levels))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — Sell side (incoming = SELL limit)
// ─────────────────────────────────────────────────────────────────────────────

// TestMatchLimit_Sell_FullFill: incoming sell qty (1 BTC) == resting bid qty (1 BTC).
//
// Expected after match:
//   - Both orders fully filled.
//   - Resting bid removed from the book.
//   - 2 events published, both EventOrderFilled.
func TestMatchLimit_Sell_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newLimitOrder("sell-001", "user-B", "BTC-USDT", engine.SideSell, 49_900, 1)

	zap.S().Infow("=== TestMatchLimit_Sell_FullFill: setup ===",
		"resting_bid_price", resting.Price.String(),
		"resting_bid_qty", resting.Quantity.String(),
		"incoming_sell_price", incoming.Price.String(),
		"incoming_sell_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_FullFill: result ===",
		"error", err,
		"events_published", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("resting bid should be removed after full fill, bid levels = %d", len(ob.Bids.Levels))
	}
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("fully-filled incoming sell should not be added to asks, ask levels = %d", len(ob.Asks.Levels))
	}
}

// TestMatchLimit_Sell_PartialFill_IncomingLarger: incoming sell qty (2 BTC) > resting bid qty (1 BTC).
//
// Expected after match:
//   - Resting bid fully filled and removed.
//   - Incoming sell partially filled (1 BTC remaining) and added to the ask side.
//   - 2 events published.
func TestMatchLimit_Sell_PartialFill_IncomingLarger(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newLimitOrder("sell-001", "user-B", "BTC-USDT", engine.SideSell, 49_900, 2)

	zap.S().Infow("=== TestMatchLimit_Sell_PartialFill_IncomingLarger: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_PartialFill_IncomingLarger: result ===",
		"error", err,
		"events_published", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("resting bid should be removed, bid levels = %d", len(ob.Bids.Levels))
	}
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("partially-filled incoming sell should be on ask side, ask levels = %d", len(ob.Asks.Levels))
	}
}

// TestMatchLimit_Sell_SelfTrade: same userID on both sides → no match.
func TestMatchLimit_Sell_SelfTrade(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	sameUser := "user-A"
	resting := newLimitOrder("bid-001", sameUser, "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newLimitOrder("sell-001", sameUser, "BTC-USDT", engine.SideSell, 49_900, 1)

	zap.S().Infow("=== TestMatchLimit_Sell_SelfTrade: setup ===", "user_id", sameUser)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_SelfTrade: result ===",
		"error", err, "events_published", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("self-trade: expected 0 events, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 1 || len(ob.Bids.Levels[0].Orders) != 1 {
		t.Errorf("self-trade: bid side should be untouched")
	}
}

// TestMatchLimit_Buy_MultipleRestingOrders: two resting asks at the same price level.
// Incoming buy qty (2 BTC) consumes both resting orders (1 BTC each).
//
// Expected:
//   - Both resting asks are fully filled and removed.
//   - Incoming order is fully filled.
//   - 4 events published (2 per matched pair — but depends on implementation).
func TestMatchLimit_Buy_MultipleRestingOrders(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting1 := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	resting2 := newLimitOrder("ask-002", "user-C", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting1, resting2)

	incoming := newLimitOrder("buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_100, 2)

	zap.S().Infow("=== TestMatchLimit_Buy_MultipleRestingOrders: setup ===",
		"resting1_id", resting1.ID, "resting2_id", resting2.ID,
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_MultipleRestingOrders: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Incoming 2 BTC consumed by two 1-BTC resting orders:
	// each iteration emits a batch of 2 events → 4 total.
	if pub.count() != 4 {
		t.Errorf("expected 4 events (2 per match iteration), got %d", pub.count())
	}
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("both resting asks should be removed, ask levels = %d", len(ob.Asks.Levels))
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("fully filled incoming should not be on bids, bid levels = %d", len(ob.Bids.Levels))
	}
}
