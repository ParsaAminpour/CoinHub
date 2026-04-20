package tests

import (
	"context"
	"testing"

	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/engine"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ─────────────────────────────────────────────
// Mock order repository
// ─────────────────────────────────────────────

type mockOrderRepository struct {
	updatedOrderID string
	updatedStatus  entities.OrderStatus
	updatedFilled  decimal.Decimal
	updateErr      error
}

func (m *mockOrderRepository) CreateOrder(_ context.Context, _ *entities.Order) error { return nil }
func (m *mockOrderRepository) GetOrderByID(_ context.Context, _ uuid.UUID) (*entities.Order, error) {
	return nil, nil
}
func (m *mockOrderRepository) UpdateOrderStatus(_ context.Context, orderID string, status entities.OrderStatus, filled decimal.Decimal) error {
	m.updatedOrderID = orderID
	m.updatedStatus = status
	m.updatedFilled = filled
	return m.updateErr
}

var _ repositories.OrderRepository = (*mockOrderRepository)(nil)

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

func newCancelOrder(id, userID, pair string, side engine.OrderSide, price float64) *engine.Order {
	o := engine.NewOrder(userID, pair, engine.OrderTypeCancel, side, d(price), decimal.Zero)
	o.ID = id
	return o
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — Cancel
// ─────────────────────────────────────────────────────────────────────────────

// TestCancel_Buy_RemovesOrderFromBids: a resting BUY order is cancelled and
// removed from the bid side. The price level must be cleaned up.
func TestCancel_Buy_RemovesOrderFromBids(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	cancel := newCancelOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000)

	zap.S().Infow("=== TestCancel_Buy_RemovesOrderFromBids: setup ===",
		"order_id", resting.ID, "price", resting.Price.String())
	logBookState(t, ob)

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	logBookState(t, ob)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(ob.Bids.Levels) != 0 {
		t.Errorf("expected bid side to be empty after cancel, got %d levels", len(ob.Bids.Levels))
	}
	if repo.updatedOrderID != "bid-001" {
		t.Errorf("expected DB update for order bid-001, got %q", repo.updatedOrderID)
	}
	if repo.updatedStatus != entities.StatusCancelled {
		t.Errorf("expected status CANCELLED, got %s", repo.updatedStatus)
	}
}

// TestCancel_Sell_RemovesOrderFromAsks: a resting SELL order is cancelled and
// removed from the ask side. The price level must be cleaned up.
func TestCancel_Sell_RemovesOrderFromAsks(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	resting := newLimitOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 51_000, 2)
	seedAsks(ob, 51_000, resting)

	cancel := newCancelOrder("ask-001", "user-B", "BTC-USDT", engine.SideSell, 51_000)

	zap.S().Infow("=== TestCancel_Sell_RemovesOrderFromAsks: setup ===",
		"order_id", resting.ID, "price", resting.Price.String())
	logBookState(t, ob)

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	logBookState(t, ob)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(ob.Asks.Levels) != 0 {
		t.Errorf("expected ask side to be empty after cancel, got %d levels", len(ob.Asks.Levels))
	}
	if repo.updatedOrderID != "ask-001" {
		t.Errorf("expected DB update for order ask-001, got %q", repo.updatedOrderID)
	}
	if repo.updatedStatus != entities.StatusCancelled {
		t.Errorf("expected status CANCELLED, got %s", repo.updatedStatus)
	}
}

// TestCancel_Buy_OrderNotFound: cancel request for an order that does not exist
// in the book — must return an error and must NOT call UpdateOrderStatus.
func TestCancel_Buy_OrderNotFound(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	// Wrong ID — not present in the book.
	cancel := newCancelOrder("bid-999", "user-A", "BTC-USDT", engine.SideBuy, 50_000)

	zap.S().Infow("=== TestCancel_Buy_OrderNotFound: setup ===", "cancel_id", cancel.ID)

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	if err == nil {
		t.Errorf("expected error for non-existent order, got nil")
	}
	if repo.updatedOrderID != "" {
		t.Errorf("UpdateOrderStatus must not be called when order is not found")
	}
	// Resting order must remain untouched.
	if len(ob.Bids.Levels) != 1 || len(ob.Bids.Levels[0].Orders) != 1 {
		t.Errorf("bid side should be untouched, got %d levels", len(ob.Bids.Levels))
	}
}

// TestCancel_Buy_WrongPrice: cancel request has a price that doesn't match any
// level — must return an error and leave the book intact.
func TestCancel_Buy_WrongPrice(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	resting := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	seedBids(ob, 50_000, resting)

	cancel := newCancelOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 49_000)

	zap.S().Infow("=== TestCancel_Buy_WrongPrice: setup ===",
		"order_price", 50_000, "cancel_price", 49_000)

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	if err == nil {
		t.Errorf("expected error when price level not found, got nil")
	}
	if repo.updatedOrderID != "" {
		t.Errorf("UpdateOrderStatus must not be called when price level not found")
	}
	if len(ob.Bids.Levels) != 1 || len(ob.Bids.Levels[0].Orders) != 1 {
		t.Errorf("bid side should be untouched")
	}
}

// TestCancel_Buy_MultipleOrdersAtLevel: two resting bids at the same price level.
// Only the targeted order must be removed; the other must remain.
func TestCancel_Buy_MultipleOrdersAtLevel(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	order1 := newLimitOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000, 1)
	order2 := newLimitOrder("bid-002", "user-B", "BTC-USDT", engine.SideBuy, 50_000, 2)
	seedBids(ob, 50_000, order1, order2)

	cancel := newCancelOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000)

	zap.S().Infow("=== TestCancel_Buy_MultipleOrdersAtLevel: setup ===",
		"cancelling", cancel.ID, "remaining", order2.ID)
	logBookState(t, ob)

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	logBookState(t, ob)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	// Level must still exist with exactly one order remaining.
	if len(ob.Bids.Levels) != 1 {
		t.Errorf("expected 1 bid level to remain, got %d", len(ob.Bids.Levels))
	}
	if len(ob.Bids.Levels[0].Orders) != 1 {
		t.Errorf("expected 1 order remaining at level, got %d", len(ob.Bids.Levels[0].Orders))
	}
	if ob.Bids.Levels[0].Orders[0].ID != "bid-002" {
		t.Errorf("expected bid-002 to remain, got %s", ob.Bids.Levels[0].Orders[0].ID)
	}
	if repo.updatedOrderID != "bid-001" {
		t.Errorf("expected DB update for bid-001, got %q", repo.updatedOrderID)
	}
}

// TestCancel_EmptyBook_Buy: cancel on an empty orderbook must return an error
// without panicking or calling UpdateOrderStatus.
func TestCancel_EmptyBook_Buy(t *testing.T) {
	initLogger(t)
	ob := newOrderbook("BTC-USDT")
	repo := &mockOrderRepository{}

	cancel := newCancelOrder("bid-001", "user-A", "BTC-USDT", engine.SideBuy, 50_000)

	zap.S().Infow("=== TestCancel_EmptyBook_Buy: setup === (empty book)")

	_, err := ob.Cancel(context.Background(), repo, *cancel)

	if err == nil {
		t.Errorf("expected error when cancelling on empty book, got nil")
	}
	if repo.updatedOrderID != "" {
		t.Errorf("UpdateOrderStatus must not be called on empty book")
	}
}
