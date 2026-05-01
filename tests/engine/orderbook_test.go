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
	mu          sync.Mutex
	events      []kafka.OrderEvent
	tradeEvents []kafka.TradeStatusEvent
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

func (p *capturePublisher) PublishTradeStatusEvent(event kafka.TradeStatusEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	zap.S().Infow("[publisher] PublishTradeStatusEvent",
		"maker_order_id", event.GetMakerOrderID(),
		"taker_order_id", event.GetTakerOrderID(),
		"pair", event.GetPair(),
		"price", event.GetPrice().String(),
		"quantity", event.GetQuantity().String(),
		"maker_filled", event.MakerFilled,
		"taker_filled", event.TakerFilled,
	)
	p.tradeEvents = append(p.tradeEvents, event)
	return nil
}

func (p *capturePublisher) PublishTradeStatusEventBatch(events []kafka.TradeStatusEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		zap.S().Infow("[publisher] PublishTradeStatusEventBatch — single trade",
			"maker_order_id", e.GetMakerOrderID(),
			"taker_order_id", e.GetTakerOrderID(),
			"pair", e.GetPair(),
			"price", e.GetPrice().String(),
			"quantity", e.GetQuantity().String(),
			"maker_filled", e.MakerFilled,
			"taker_filled", e.TakerFilled,
		)
	}
	p.tradeEvents = append(p.tradeEvents, events...)
	return nil
}

func (p *capturePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *capturePublisher) tradeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tradeEvents)
}

func (p *capturePublisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = nil
	p.tradeEvents = nil
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

// newOrderbook creates an Orderbook for the given pair with initialized BTree
// sides and a small dust threshold so near-zero remainders are treated as filled.
func newOrderbook(pair string) *engine.Orderbook {
	return &engine.Orderbook{
		Pair: pair,
		Dust: d(0.0001),
		Asks: engine.NewSide(32, true),
		Bids: engine.NewSide(32, false),
	}
}

// seedAsks inserts a price level with the given orders into the ask side BTree.
func seedAsks(ob *engine.Orderbook, price float64, orders ...*engine.Order) {
	level := &engine.PriceLevel{
		PriceLevel: d(price),
		Orders:     orders,
	}
	ob.Asks.Levels.ReplaceOrInsert(level)
}

// seedBids inserts a price level with the given orders into the bid side BTree.
func seedBids(ob *engine.Orderbook, price float64, orders ...*engine.Order) {
	level := &engine.PriceLevel{
		PriceLevel: d(price),
		Orders:     orders,
	}
	ob.Bids.Levels.ReplaceOrInsert(level)
}

// firstAskLevel returns the best (lowest price) ask level from the BTree, or nil.
func firstAskLevel(ob *engine.Orderbook) *engine.PriceLevel {
	var first *engine.PriceLevel
	ob.Asks.Levels.Ascend(func(p *engine.PriceLevel) bool {
		first = p
		return false
	})
	return first
}

// firstBidLevel returns the best (highest price) bid level from the BTree, or nil.
func firstBidLevel(ob *engine.Orderbook) *engine.PriceLevel {
	var first *engine.PriceLevel
	ob.Bids.Levels.Ascend(func(p *engine.PriceLevel) bool {
		first = p
		return false
	})
	return first
}

func logBookState(t *testing.T, ob *engine.Orderbook) {
	t.Helper()
	zap.S().Infow("[book state]",
		"pair", ob.Pair,
		"ask_levels", ob.Asks.Levels.Len(),
		"bid_levels", ob.Bids.Levels.Len(),
	)
	ob.Asks.Levels.Ascend(func(lvl *engine.PriceLevel) bool {
		zap.S().Infow("[ask level]", "price", lvl.PriceLevel.String(), "order_count", len(lvl.Orders))
		for j, o := range lvl.Orders {
			zap.S().Infow("[ask order]", "order_idx", j,
				"id", o.ID, "qty", o.Quantity.String(), "filled", o.Filled.String(), "remaining", o.Remaining().String())
		}
		return true
	})
	ob.Bids.Levels.Ascend(func(lvl *engine.PriceLevel) bool {
		zap.S().Infow("[bid level]", "price", lvl.PriceLevel.String(), "order_count", len(lvl.Orders))
		for j, o := range lvl.Orders {
			zap.S().Infow("[bid order]", "order_idx", j,
				"id", o.ID, "qty", o.Quantity.String(), "filled", o.Filled.String(), "remaining", o.Remaining().String())
		}
		return true
	})
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
		"ask_levels_before", ob.Asks.Levels.Len(),
	)

	err := ob.MatchLimit(pub, *incoming)

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
	if pub.tradeCount() != 0 {
		t.Errorf("expected 0 trade events, got %d", pub.tradeCount())
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
		"bid_levels_before", ob.Bids.Levels.Len(),
	)

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_EmptyBids: result ===", "error", err, "events", pub.count())

	if err != engine.ErrOrderbookEmpty {
		t.Errorf("expected ErrOrderbookEmpty, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events published, got %d", pub.count())
	}
	if pub.tradeCount() != 0 {
		t.Errorf("expected 0 trade events, got %d", pub.tradeCount())
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PriceNoMatch: result ===",
		"error", err, "events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len())

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("expected 0 events (no match), got %d", pub.count())
	}
	if pub.tradeCount() != 0 {
		t.Errorf("expected 0 trade events (no match), got %d", pub.tradeCount())
	}
	if ob.Asks.Levels.Len() != 1 {
		t.Errorf("ask side should be untouched, got %d levels", ob.Asks.Levels.Len())
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_PriceNoMatch: result ===",
		"error", err, "events_published", pub.count())

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// A cancel event must be published for the incoming order.
	if pub.count() != 1 {
		t.Errorf("expected 1 cancel event, got %d", pub.count())
	}
	// No fill happened → no trade events.
	if pub.tradeCount() != 0 {
		t.Errorf("expected 0 trade events (cancel, no fill), got %d", pub.tradeCount())
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_SelfTrade: result ===",
		"error", err, "events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len(),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Self-trade prevention: the resting order was skipped, so nothing should change.
	if pub.count() != 0 {
		t.Errorf("self-trade: expected 0 events published, got %d", pub.count())
	}
	if pub.tradeCount() != 0 {
		t.Errorf("self-trade: expected 0 trade events, got %d", pub.tradeCount())
	}
	lvl := firstAskLevel(ob)
	if ob.Asks.Levels.Len() != 1 || lvl == nil || len(lvl.Orders) != 1 {
		t.Errorf("self-trade: ask side must be untouched, got %d levels", ob.Asks.Levels.Len())
	}
}

// TestMatchLimit_Buy_FullFill: incoming buy qty == resting ask qty (both = 1 BTC).
// Price: best ask = 50 000, incoming limit = 50 100 → price matches.
//
// Expected after match:
//   - Both orders are fully filled.
//   - The resting ask is removed from the book (Asks empty).
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_FullFill: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len(),
		"bid_levels_after", ob.Bids.Levels.Len(),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	// Both orders are fully matched → 2 order events must be published.
	if pub.count() != 2 {
		t.Errorf("expected 2 events (one per order), got %d", pub.count())
	}
	// Resting ask must be removed from the book.
	if ob.Asks.Levels.Len() != 0 {
		t.Errorf("resting ask should be removed after full fill, ask levels = %d", ob.Asks.Levels.Len())
	}
	// Incoming buy is fully filled → must NOT be added to the bid side.
	if ob.Bids.Levels.Len() != 0 {
		t.Errorf("incoming order should NOT be added to bids after full fill, bid levels = %d", ob.Bids.Levels.Len())
	}
	// Exactly 1 trade event emitted for the single fill.
	if pub.tradeCount() != 1 {
		t.Errorf("expected 1 trade event, got %d", pub.tradeCount())
	} else {
		te := pub.tradeEvents[0]
		zap.S().Infow("[trade event check]",
			"maker_order_id", te.GetMakerOrderID(),
			"taker_order_id", te.GetTakerOrderID(),
			"price", te.GetPrice().String(),
			"quantity", te.GetQuantity().String(),
			"maker_filled", te.MakerFilled,
			"taker_filled", te.TakerFilled,
		)
		if te.GetMakerOrderID() != "ask-001" {
			t.Errorf("trade: expected maker=ask-001, got %s", te.GetMakerOrderID())
		}
		if te.GetTakerOrderID() != "buy-001" {
			t.Errorf("trade: expected taker=buy-001, got %s", te.GetTakerOrderID())
		}
		if !te.GetQuantity().Equal(d(1)) {
			t.Errorf("trade: expected quantity=1, got %s", te.GetQuantity().String())
		}
		if !te.MakerFilled {
			t.Errorf("trade: expected MakerFilled=true")
		}
		if !te.TakerFilled {
			t.Errorf("trade: expected TakerFilled=true")
		}
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingLarger: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len(),
		"bid_levels_after", ob.Bids.Levels.Len(),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask (qty=1) is fully consumed → ask side must be empty.
	if ob.Asks.Levels.Len() != 0 {
		t.Errorf("resting ask should be removed, ask levels = %d", ob.Asks.Levels.Len())
	}
	// Incoming still has 1 BTC remaining → must be resting on the bid side.
	if ob.Bids.Levels.Len() != 1 {
		t.Errorf("incoming partial order should be added to bids, bid levels = %d", ob.Bids.Levels.Len())
	}
	// 1 trade event: resting was fully filled, incoming was not.
	if pub.tradeCount() != 1 {
		t.Errorf("expected 1 trade event, got %d", pub.tradeCount())
	} else {
		te := pub.tradeEvents[0]
		zap.S().Infow("[trade event check]",
			"maker_order_id", te.GetMakerOrderID(), "taker_order_id", te.GetTakerOrderID(),
			"quantity", te.GetQuantity().String(), "maker_filled", te.MakerFilled, "taker_filled", te.TakerFilled,
		)
		if !te.GetQuantity().Equal(d(1)) {
			t.Errorf("trade: expected quantity=1 (resting fully consumed), got %s", te.GetQuantity().String())
		}
		if !te.MakerFilled {
			t.Errorf("trade: resting ask was fully consumed — MakerFilled should be true")
		}
		if te.TakerFilled {
			t.Errorf("trade: incoming still has remainder — TakerFilled should be false")
		}
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_PartialFill_IncomingSmaller: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len(),
		"bid_levels_after", ob.Bids.Levels.Len(),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	// Resting ask is only partially consumed → must still be in the book.
	if ob.Asks.Levels.Len() != 1 {
		t.Errorf("resting ask should remain in book after partial fill, ask levels = %d", ob.Asks.Levels.Len())
	}
	lvl := firstAskLevel(ob)
	if lvl == nil || len(lvl.Orders) == 0 {
		t.Fatal("expected a resting order in the ask level")
	}
	restingAfter := lvl.Orders[0]
	expectedRemaining := d(0.5)
	if !restingAfter.Remaining().Equal(expectedRemaining) {
		t.Errorf("resting ask remaining should be 0.5, got %s", restingAfter.Remaining().String())
	}
	// Incoming is fully filled → must NOT be added to bids.
	if ob.Bids.Levels.Len() != 0 {
		t.Errorf("fully-filled incoming should not be added to bids, bid levels = %d", ob.Bids.Levels.Len())
	}
	// 1 trade event: incoming fully consumed, resting partially consumed.
	if pub.tradeCount() != 1 {
		t.Errorf("expected 1 trade event, got %d", pub.tradeCount())
	} else {
		te := pub.tradeEvents[0]
		zap.S().Infow("[trade event check]",
			"quantity", te.GetQuantity().String(), "maker_filled", te.MakerFilled, "taker_filled", te.TakerFilled,
		)
		if !te.GetQuantity().Equal(d(0.5)) {
			t.Errorf("trade: expected quantity=0.5, got %s", te.GetQuantity().String())
		}
		if te.MakerFilled {
			t.Errorf("trade: resting ask still has remainder — MakerFilled should be false")
		}
		if !te.TakerFilled {
			t.Errorf("trade: incoming was fully consumed — TakerFilled should be true")
		}
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_FullFill: result ===",
		"error", err,
		"events_published", pub.count(),
		"bid_levels_after", ob.Bids.Levels.Len(),
		"ask_levels_after", ob.Asks.Levels.Len(),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if ob.Bids.Levels.Len() != 0 {
		t.Errorf("resting bid should be removed after full fill, bid levels = %d", ob.Bids.Levels.Len())
	}
	if ob.Asks.Levels.Len() != 0 {
		t.Errorf("fully-filled incoming sell should not be added to asks, ask levels = %d", ob.Asks.Levels.Len())
	}
	// 1 trade event: both sides fully filled.
	if pub.tradeCount() != 1 {
		t.Errorf("expected 1 trade event, got %d", pub.tradeCount())
	} else {
		te := pub.tradeEvents[0]
		zap.S().Infow("[trade event check]",
			"maker_order_id", te.GetMakerOrderID(), "taker_order_id", te.GetTakerOrderID(),
			"quantity", te.GetQuantity().String(), "maker_filled", te.MakerFilled, "taker_filled", te.TakerFilled,
		)
		if !te.GetQuantity().Equal(d(1)) {
			t.Errorf("trade: expected quantity=1, got %s", te.GetQuantity().String())
		}
		if !te.MakerFilled {
			t.Errorf("trade: MakerFilled should be true")
		}
		if !te.TakerFilled {
			t.Errorf("trade: TakerFilled should be true")
		}
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_PartialFill_IncomingLarger: result ===",
		"error", err,
		"events_published", pub.count(),
		"bid_levels_after", ob.Bids.Levels.Len(),
		"ask_levels_after", ob.Asks.Levels.Len(),
	)
	logBookState(t, ob)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 2 {
		t.Errorf("expected 2 events, got %d", pub.count())
	}
	if ob.Bids.Levels.Len() != 0 {
		t.Errorf("resting bid should be removed, bid levels = %d", ob.Bids.Levels.Len())
	}
	if ob.Asks.Levels.Len() != 1 {
		t.Errorf("partially-filled incoming sell should be on ask side, ask levels = %d", ob.Asks.Levels.Len())
	}
	// 1 trade event: resting bid fully consumed, incoming sell has remainder.
	if pub.tradeCount() != 1 {
		t.Errorf("expected 1 trade event, got %d", pub.tradeCount())
	} else {
		te := pub.tradeEvents[0]
		zap.S().Infow("[trade event check]",
			"quantity", te.GetQuantity().String(), "maker_filled", te.MakerFilled, "taker_filled", te.TakerFilled,
		)
		if !te.GetQuantity().Equal(d(1)) {
			t.Errorf("trade: expected quantity=1, got %s", te.GetQuantity().String())
		}
		if !te.MakerFilled {
			t.Errorf("trade: resting bid fully consumed — MakerFilled should be true")
		}
		if te.TakerFilled {
			t.Errorf("trade: incoming sell has remainder — TakerFilled should be false")
		}
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Sell_SelfTrade: result ===",
		"error", err, "events_published", pub.count(),
		"bid_levels_after", ob.Bids.Levels.Len(),
	)

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if pub.count() != 0 {
		t.Errorf("self-trade: expected 0 events, got %d", pub.count())
	}
	if pub.tradeCount() != 0 {
		t.Errorf("self-trade: expected 0 trade events, got %d", pub.tradeCount())
	}
	lvl := firstBidLevel(ob)
	if ob.Bids.Levels.Len() != 1 || lvl == nil || len(lvl.Orders) != 1 {
		t.Errorf("self-trade: bid side should be untouched")
	}
}

// TestMatchLimit_Buy_MultipleRestingOrders: two resting asks at the same price level.
// Incoming buy qty (2 BTC) consumes both resting orders (1 BTC each).
//
// Expected:
//   - Both resting asks are fully filled and removed.
//   - Incoming order is fully filled.
//   - 4 events published (2 per matched pair).
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

	err := ob.MatchLimit(pub, *incoming)

	zap.S().Infow("=== TestMatchLimit_Buy_MultipleRestingOrders: result ===",
		"error", err,
		"events_published", pub.count(),
		"ask_levels_after", ob.Asks.Levels.Len(),
		"bid_levels_after", ob.Bids.Levels.Len(),
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
	if ob.Asks.Levels.Len() != 0 {
		t.Errorf("both resting asks should be removed, ask levels = %d", ob.Asks.Levels.Len())
	}
	if ob.Bids.Levels.Len() != 0 {
		t.Errorf("fully filled incoming should not be on bids, bid levels = %d", ob.Bids.Levels.Len())
	}
	// 2 trade events — one per resting order matched.
	if pub.tradeCount() != 2 {
		t.Errorf("expected 2 trade events (one per fill iteration), got %d", pub.tradeCount())
	} else {
		// First fill: ask-001 consumed, incoming partial → MakerFilled=true, TakerFilled=false.
		te1 := pub.tradeEvents[0]
		zap.S().Infow("[trade 1 check]",
			"maker_order_id", te1.GetMakerOrderID(), "quantity", te1.GetQuantity().String(),
			"maker_filled", te1.MakerFilled, "taker_filled", te1.TakerFilled,
		)
		if te1.GetMakerOrderID() != "ask-001" {
			t.Errorf("trade[0]: expected maker=ask-001, got %s", te1.GetMakerOrderID())
		}
		if !te1.MakerFilled {
			t.Errorf("trade[0]: ask-001 fully consumed — MakerFilled should be true")
		}
		if te1.TakerFilled {
			t.Errorf("trade[0]: incoming still has remainder after first fill — TakerFilled should be false")
		}
		// Second fill: ask-002 consumed, incoming fully filled → both true.
		te2 := pub.tradeEvents[1]
		zap.S().Infow("[trade 2 check]",
			"maker_order_id", te2.GetMakerOrderID(), "quantity", te2.GetQuantity().String(),
			"maker_filled", te2.MakerFilled, "taker_filled", te2.TakerFilled,
		)
		if te2.GetMakerOrderID() != "ask-002" {
			t.Errorf("trade[1]: expected maker=ask-002, got %s", te2.GetMakerOrderID())
		}
		if !te2.MakerFilled {
			t.Errorf("trade[1]: ask-002 fully consumed — MakerFilled should be true")
		}
		if !te2.TakerFilled {
			t.Errorf("trade[1]: incoming fully consumed — TakerFilled should be true")
		}
	}
}
