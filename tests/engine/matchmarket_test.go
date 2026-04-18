package tests

// ─────────────────────────────────────────────────────────────────────────────
// MatchMarket tests
//
// Key differences from MatchLimit:
//   - sweeps ALL qualifying price levels (multi-level sweep), not just the best
//   - the incoming order never rests in the book — unfilled remainder is cancelled
//   - events for all resting fills are batched and published together when the
//     incoming order is fully consumed
//
// Test naming: TestMatchMarket_<Side>_<Scenario>
// ─────────────────────────────────────────────────────────────────────────────

import (
	"testing"

	"coinhub/internal/engine"

	"go.uber.org/zap"
)

// newMarketOrder wraps newLimitOrder with OrderTypeMarket.
// For a market buy, set price high enough to cover all relevant ask levels.
// For a market sell, set price low enough to be below all relevant bid levels.
func newMarketOrder(id, userID, pair string, side engine.OrderSide, price, qty float64) *engine.Order {
	o := engine.NewOrder(userID, pair, engine.OrderTypeMarket, side, d(price), d(qty))
	o.ID = id
	return o
}

// ─────────────────────────────────────────────────────────────────────────────
// Buy side
// ─────────────────────────────────────────────────────────────────────────────

// TestMatchMarket_Buy_EmptyAsks: no resting asks → ErrOrderbookEmpty.
func TestMatchMarket_Buy_EmptyAsks(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_EmptyAsks: setup ===",
		"incoming_id", incoming.ID, "incoming_price", incoming.Price.String(),
		"ask_levels", len(ob.Asks.Levels),
	)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_EmptyAsks: result ===", "error", err, "events", pub.count())

	if err != engine.ErrOrderbookEmpty {
		t.Errorf("expected ErrOrderbookEmpty, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events, got %d", pub.count())
	}
}

// TestMatchMarket_Buy_PriceNoMatch: the only ask level is above the incoming price.
// Expectation: incoming is cancelled immediately, 1 cancel event published.
func TestMatchMarket_Buy_PriceNoMatch(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 55_000, 1)
	seedAsks(ob, 55_000, resting)

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_PriceNoMatch: setup ===",
		"ask_price", 55_000, "incoming_price", 50_000)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_PriceNoMatch: result ===",
		"error", err, "events", pub.count())

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Incoming must be cancelled → 1 cancel event.
	if pub.count() != 1 {
		t.Errorf("expected 1 cancel event, got %d", pub.count())
	}
	// Ask side must be untouched.
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("ask side should be untouched, got %d levels", len(ob.Asks.Levels))
	}
}

// TestMatchMarket_Buy_FullFill: incoming market buy (qty=1) against one resting ask (qty=1).
// Expectation:
//   - Both orders fully filled.
//   - Ask side cleared.
//   - 2 events published (resting filled + incoming filled in one batch).
func TestMatchMarket_Buy_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_FullFill: setup ===",
		"resting_id", resting.ID, "resting_qty", resting.Quantity.String(),
		"incoming_id", incoming.ID, "incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_FullFill: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// 2 events: resting filled + incoming filled (published as a batch).
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask fully consumed.
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("ask side should be empty after full fill, got %d levels", len(ob.Asks.Levels))
	}
}

// TestMatchMarket_Buy_PartialFill_IncomingSmaller: incoming qty (0.5) < resting qty (1).
// Expectation:
//   - Incoming is fully filled (consumed entirely).
//   - Resting ask is partially filled and remains in the book (qty=0.5 remaining).
//   - 2 events published.
func TestMatchMarket_Buy_PartialFill_IncomingSmaller(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 0.5)

	zap.S().Infow("=== TestMatchMarket_Buy_PartialFill_IncomingSmaller: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_PartialFill_IncomingSmaller: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask is only partially consumed — must remain.
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("resting ask should remain in book, got %d levels", len(ob.Asks.Levels))
	}
	restingAfter := ob.Asks.Levels[0].Orders[0]
	if !restingAfter.Remaining().Equal(d(0.5)) {
		t.Errorf("resting ask remaining should be 0.5, got %s", restingAfter.Remaining().String())
	}
}

// TestMatchMarket_Buy_PartialFill_IncomingLarger: incoming qty (2) > resting qty (1), only one ask level.
// The incoming absorbs the resting order but there are no more levels to fill from.
// Expectation:
//   - Resting ask fully filled and removed.
//   - Incoming order partially filled (1 remaining), and the remaining is CANCELLED
//     (market orders don't rest in the book).
//   - Events: 2 (resting filled + incoming filled/cancelled).
func TestMatchMarket_Buy_PartialFill_IncomingLarger(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 2)

	zap.S().Infow("=== TestMatchMarket_Buy_PartialFill_IncomingLarger: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_PartialFill_IncomingLarger: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Resting fully consumed → ask side empty.
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("resting ask should be removed, got %d ask levels", len(ob.Asks.Levels))
	}
	// Events: at minimum 2 (resting filled + incoming partial/cancelled).
	if pub.count() < 2 {
		t.Errorf("expected at least 2 events, got %d", pub.count())
	}
	// Market orders must NOT be placed on the bid side.
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("market order should never rest on bids, got %d bid levels", len(ob.Bids.Levels))
	}
}

// TestMatchMarket_Buy_MultiLevel_FullFill: two ask levels both within the incoming price.
// incoming qty=1, level-1 has 0.5 BTC @49000, level-2 has 0.5 BTC @50000.
// Expectation:
//   - Both levels fully consumed.
//   - Incoming fully filled.
//   - 3 events published (resting-1 filled + resting-2 filled + incoming filled in one batch).
func TestMatchMarket_Buy_MultiLevel_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	// Two price levels; asks are ascending so lower price first.
	resting1 := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 49_000, 0.5)
	resting2 := newLimitOrder("ask-002", "user-C", "BTC-USDT", engine.SideSell, 50_000, 0.5)
	// Manually build two levels to guarantee ordering.
	ob.Asks.Levels = []*engine.PriceLevel{
		{PriceLevel: d(49_000), Orders: []*engine.Order{resting1}},
		{PriceLevel: d(50_000), Orders: []*engine.Order{resting2}},
	}

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_MultiLevel_FullFill: setup ===",
		"level1_price", 49_000, "level1_qty", resting1.Quantity.String(),
		"level2_price", 50_000, "level2_qty", resting2.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_MultiLevel_FullFill: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Both ask levels consumed.
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("all ask levels should be consumed, got %d", len(ob.Asks.Levels))
	}
	// 3 events: resting1 filled + resting2 filled + incoming filled (one batch).
	if pub.count() != 3 {
		t.Errorf("expected 3 events (2 resting + 1 incoming), got %d", pub.count())
	}
}

// TestMatchMarket_Buy_MultiLevel_SecondLevelPriceTooHigh:
// level-1 @49000 matches (partial fill), level-2 @56000 is above incoming price.
// Expectation:
//   - Level-1 resting fully consumed.
//   - Incoming partially filled; remaining is CANCELLED when it hits the too-expensive level.
//   - At least 2 events: resting-1 filled + incoming partial/cancel.
func TestMatchMarket_Buy_MultiLevel_SecondLevelPriceTooHigh(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting1 := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 49_000, 0.5)
	resting2 := newLimitOrder("ask-002", "user-C", "BTC-USDT", engine.SideSell, 56_000, 0.5) // too expensive
	ob.Asks.Levels = []*engine.PriceLevel{
		{PriceLevel: d(49_000), Orders: []*engine.Order{resting1}},
		{PriceLevel: d(56_000), Orders: []*engine.Order{resting2}},
	}

	incoming := newMarketOrder("mkt-buy-001", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_MultiLevel_SecondLevelPriceTooHigh: setup ===",
		"level1_price", 49_000, "level2_price", 56_000, "incoming_price", 55_000,
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_MultiLevel_SecondLevelPriceTooHigh: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Level-1 is consumed.
	// Level-2 remains (incoming cancelled before touching it).
	if len(ob.Asks.Levels) != 1 {
		t.Errorf("level-2 should remain untouched, got %d ask levels", len(ob.Asks.Levels))
	}
	// At least 2 events (resting-1 filled + incoming partial/cancel).
	if pub.count() < 2 {
		t.Errorf("expected at least 2 events, got %d", pub.count())
	}
}

// TestMatchMarket_Buy_SelfTrade: market order matches against an ask with the same order ID.
// NOTE: the buy side checks restingOrder.ID == incomingOrder.ID (not UserID).
// Expectation: the resting order is skipped, no fill.
func TestMatchMarket_Buy_SelfTrade(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	// resting order with same ID as incoming
	resting := newLimitOrder("order-same-id", "user-B", "BTC-USDT", engine.SideSell, 50_000, 1)
	seedAsks(ob, 50_000, resting)

	incoming := newMarketOrder("order-same-id", "user-A", "BTC-USDT", engine.SideBuy, 55_000, 1)

	zap.S().Infow("=== TestMatchMarket_Buy_SelfTrade: setup ===", "shared_order_id", "order-same-id")
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Buy_SelfTrade: result ===",
		"error", err, "events", pub.count(),
		"ask_levels_after", len(ob.Asks.Levels),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("self-trade: expected 0 events, got %d", pub.count())
	}
	if len(ob.Asks.Levels) != 1 || len(ob.Asks.Levels[0].Orders) != 1 {
		t.Errorf("self-trade: ask side should be untouched")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Sell side
// ─────────────────────────────────────────────────────────────────────────────

// TestMatchMarket_Sell_EmptyBids: no resting bids → ErrOrderbookEmpty.
func TestMatchMarket_Sell_EmptyBids(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	incoming := newMarketOrder("mkt-sell-001", "user-B", "BTC-USDT", engine.SideSell, 45_000, 1)

	zap.S().Infow("=== TestMatchMarket_Sell_EmptyBids: setup ===",
		"bid_levels", len(ob.Bids.Levels))

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_EmptyBids: result ===", "error", err, "events", pub.count())

	if err != engine.ErrOrderbookEmpty {
		t.Errorf("expected ErrOrderbookEmpty, got %v", err)
	}
}

// TestMatchMarket_Sell_PriceNoMatch: best bid (45 000) is below the incoming sell price (49 000).
// Expectation: incoming cancelled, 1 cancel event.
func TestMatchMarket_Sell_PriceNoMatch(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 45_000, 1)
	seedBids(ob, 45_000, resting)

	incoming := newMarketOrder("mkt-sell-001", "user-B", "BTC-USDT", engine.SideSell, 49_000, 1)

	zap.S().Infow("=== TestMatchMarket_Sell_PriceNoMatch: setup ===",
		"best_bid", 45_000, "incoming_sell_price", 49_000)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_PriceNoMatch: result ===",
		"error", err, "events", pub.count())

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 1 {
		t.Errorf("expected 1 cancel event, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 1 {
		t.Errorf("bid side should be untouched, got %d levels", len(ob.Bids.Levels))
	}
}

// TestMatchMarket_Sell_FullFill: incoming market sell (qty=1) vs resting bid (qty=1).
// Expectation: both fully filled, bid side cleared, 2 events.
func TestMatchMarket_Sell_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-sell-001", "user-B", "BTC-USDT", engine.SideSell, 45_000, 1)

	zap.S().Infow("=== TestMatchMarket_Sell_FullFill: setup ===",
		"resting_id", resting.ID, "resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_FullFill: result ===",
		"error", err, "events", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("bid side should be empty after full fill, got %d levels", len(ob.Bids.Levels))
	}
}

// TestMatchMarket_Sell_PartialFill_IncomingSmaller: incoming qty (0.5) < resting qty (1).
// Expectation: incoming fully filled, resting partial (0.5 remaining), 2 events.
func TestMatchMarket_Sell_PartialFill_IncomingSmaller(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-sell-001", "user-B", "BTC-USDT", engine.SideSell, 45_000, 0.5)

	zap.S().Infow("=== TestMatchMarket_Sell_PartialFill_IncomingSmaller: setup ===",
		"resting_qty", resting.Quantity.String(),
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_PartialFill_IncomingSmaller: result ===",
		"error", err, "events", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if len(ob.Bids.Levels) != 1 {
		t.Errorf("resting bid should remain, got %d levels", len(ob.Bids.Levels))
	}
	restingAfter := ob.Bids.Levels[0].Orders[0]
	if !restingAfter.Remaining().Equal(d(0.5)) {
		t.Errorf("resting bid remaining should be 0.5, got %s", restingAfter.Remaining().String())
	}
}

// TestMatchMarket_Sell_MultiLevel_FullFill: two bid levels both at/above incoming sell price.
// incoming qty=1, level-1 has 0.5 BTC @51000, level-2 has 0.5 BTC @50000.
// Expectation: both levels consumed, incoming fully filled, 3 events.
func TestMatchMarket_Sell_MultiLevel_FullFill(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	// Bids are descending: highest price first.
	resting1 := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 51_000, 0.5)
	resting2 := newLimitOrder("bid-002", "user-C", "BTC-USDT", engine.SideBuy, 50_000, 0.5)
	ob.Bids.Levels = []*engine.PriceLevel{
		{PriceLevel: d(51_000), Orders: []*engine.Order{resting1}},
		{PriceLevel: d(50_000), Orders: []*engine.Order{resting2}},
	}

	incoming := newMarketOrder("mkt-sell-001", "user-B", "BTC-USDT", engine.SideSell, 45_000, 1)

	zap.S().Infow("=== TestMatchMarket_Sell_MultiLevel_FullFill: setup ===",
		"level1_price", 51_000, "level2_price", 50_000,
		"incoming_qty", incoming.Quantity.String(),
	)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_MultiLevel_FullFill: result ===",
		"error", err, "events", pub.count(),
		"bid_levels_after", len(ob.Bids.Levels),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("all bid levels should be consumed, got %d", len(ob.Bids.Levels))
	}
	// 3 events: resting1 filled + resting2 filled + incoming filled.
	if pub.count() != 3 {
		t.Errorf("expected 3 events (2 resting + 1 incoming), got %d", pub.count())
	}
}

// TestMatchMarket_Sell_SelfTrade: incoming sell and resting bid share the same userID.
// Expectation: resting order skipped, no fill, no events.
func TestMatchMarket_Sell_SelfTrade(t *testing.T) {
	initLogger(t)
	pub := &capturePublisher{}
	ob := newOrderbook("BTC-USDT")

	sameUser := "user-A"
	resting := newLimitOrder("bid-001", sameUser, "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	incoming := newMarketOrder("mkt-sell-001", sameUser, "BTC-USDT", engine.SideSell, 45_000, 1)

	zap.S().Infow("=== TestMatchMarket_Sell_SelfTrade: setup ===", "user_id", sameUser)
	logBookState(t, ob)

	_, err := ob.MatchMarket(pub, *incoming)

	zap.S().Infow("=== TestMatchMarket_Sell_SelfTrade: result ===",
		"error", err, "events", pub.count(), "bid_levels_after", len(ob.Bids.Levels))

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
