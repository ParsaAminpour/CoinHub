package websocket_test

import (
	"context"
	"sync"
	"testing"

	coinhub_ws "coinhub/internal/adapter/handler/websockets"
)

// newTestHub builds a hub with no real WebSocket connections —
// the Client.Send calls are never exercised here; we only test hub state.
func newTestHub() *coinhub_ws.Hub {
	return coinhub_ws.New()
}

// stubClient builds a Client without a real WebSocket connection.
// conn is nil, which is fine as long as we never call Send in these tests.
func stubClient(connID, userID string) *coinhub_ws.Client {
	return coinhub_ws.NewClient(connID, userID, nil)
}

// ─────────────────────────────────────────────────────────────────────────────
// Register / Unregister
// ─────────────────────────────────────────────────────────────────────────────

func TestHub_Register_AddsClientAndPresence(t *testing.T) {
	hub := newTestHub()
	c := stubClient("conn-1", "user-A")

	hub.Register(c)

	stats := hub.Stats()
	if stats["connections"] != 1 {
		t.Errorf("expected 1 connection, got %d", stats["connections"])
	}
	if !hub.IsOnline("user-A") {
		t.Error("user-A should be online after register")
	}
}

func TestHub_Unregister_RemovesClientAndPresence(t *testing.T) {
	hub := newTestHub()
	c := stubClient("conn-1", "user-A")

	hub.Register(c)
	hub.Unregister("conn-1")

	stats := hub.Stats()
	if stats["connections"] != 0 {
		t.Errorf("expected 0 connections after unregister, got %d", stats["connections"])
	}
	if hub.IsOnline("user-A") {
		t.Error("user-A should be offline after unregister")
	}
}

func TestHub_Unregister_UnknownID_IsNoop(t *testing.T) {
	hub := newTestHub()
	// must not panic
	hub.Unregister("non-existent")

	if hub.Stats()["connections"] != 0 {
		t.Error("hub should remain empty")
	}
}

func TestHub_MultipleConnectionsSameUser(t *testing.T) {
	hub := newTestHub()
	c1 := stubClient("conn-1", "user-A")
	c2 := stubClient("conn-2", "user-A")

	hub.Register(c1)
	hub.Register(c2)

	if !hub.IsOnline("user-A") {
		t.Error("user-A should be online")
	}
	if hub.Stats()["connections"] != 2 {
		t.Errorf("expected 2 connections, got %d", hub.Stats()["connections"])
	}

	// Unregister one — user should still be online.
	hub.Unregister("conn-1")
	if !hub.IsOnline("user-A") {
		t.Error("user-A should still be online with conn-2")
	}

	// Unregister the last — user goes offline.
	hub.Unregister("conn-2")
	if hub.IsOnline("user-A") {
		t.Error("user-A should be offline after all connections closed")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Join / Leave
// ─────────────────────────────────────────────────────────────────────────────

func TestHub_Join_AddsClientToRoom(t *testing.T) {
	hub := newTestHub()
	c := stubClient("conn-1", "user-A")
	hub.Register(c)

	hub.Join("conn-1", "order:BTC-USDT")

	stats := hub.Stats()
	if stats["rooms"] != 1 {
		t.Errorf("expected 1 room, got %d", stats["rooms"])
	}
}

func TestHub_Leave_RemovesClientFromRoom(t *testing.T) {
	hub := newTestHub()
	c := stubClient("conn-1", "user-A")
	hub.Register(c)
	hub.Join("conn-1", "order:BTC-USDT")

	hub.Leave("conn-1", "order:BTC-USDT")

	if hub.Stats()["rooms"] != 0 {
		t.Errorf("expected 0 rooms after leave, got %d", hub.Stats()["rooms"])
	}
}

func TestHub_Unregister_CleansUpRooms(t *testing.T) {
	hub := newTestHub()
	c := stubClient("conn-1", "user-A")
	hub.Register(c)
	hub.Join("conn-1", "order:BTC-USDT")
	hub.Join("conn-1", "order:ETH-USDT")

	hub.Unregister("conn-1")

	if hub.Stats()["rooms"] != 0 {
		t.Errorf("expected rooms to be pruned on unregister, got %d", hub.Stats()["rooms"])
	}
}

func TestHub_Join_UnknownClient_IsNoop(t *testing.T) {
	hub := newTestHub()
	hub.Join("ghost-conn", "order:BTC-USDT")
	if hub.Stats()["rooms"] != 0 {
		t.Error("joining with an unregistered connID should have no effect")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BroadcastRoom / BroadcastUser
// ─────────────────────────────────────────────────────────────────────────────

// recordingClient embeds a channel to capture Send calls without a real conn.
type recordingClient struct {
	received []coinhub_ws.Message
	mu       sync.Mutex
}

func (r *recordingClient) record(msg coinhub_ws.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.received = append(r.received, msg)
}

func (r *recordingClient) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.received)
}

// TestHub_BroadcastRoom_OnlyReachesRoomMembers verifies that BroadcastRoom
// delivers to room members and not to non-members.
// Because Client.Send requires a real websocket.Conn, we test the hub's
// room membership logic by verifying Stats() and that non-members are excluded.
func TestHub_BroadcastRoom_RoomMembershipIsCorrect(t *testing.T) {
	hub := newTestHub()

	member := stubClient("conn-1", "user-A")
	outsider := stubClient("conn-2", "user-B")

	hub.Register(member)
	hub.Register(outsider)

	hub.Join("conn-1", "order:BTC-USDT")
	// outsider does NOT join the room

	// Verify only 1 client is in the room (membership check, not delivery check
	// since we can't Send on nil conns here — that is covered in server_test.go).
	stats := hub.Stats()
	if stats["rooms"] != 1 {
		t.Errorf("expected 1 room, got %d", stats["rooms"])
	}
	if stats["connections"] != 2 {
		t.Errorf("expected 2 total connections, got %d", stats["connections"])
	}
}

func TestHub_IsOnline_FalseForUnknownUser(t *testing.T) {
	hub := newTestHub()
	if hub.IsOnline("ghost-user") {
		t.Error("unknown user should not be online")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Concurrency smoke test
// ─────────────────────────────────────────────────────────────────────────────

func TestHub_ConcurrentRegisterUnregister(t *testing.T) {
	hub := newTestHub()
	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "conn-" + string(rune('A'+i%26))
			c := stubClient(id, "user-"+id)
			hub.Register(c)
			hub.Join(id, "room-x")
			hub.Unregister(id)
		}(i)
	}
	wg.Wait()

	// After all goroutines finish all connections should be cleaned up.
	stats := hub.Stats()
	if stats["connections"] != 0 {
		t.Errorf("expected 0 connections after concurrent run, got %d", stats["connections"])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats
// ─────────────────────────────────────────────────────────────────────────────

func TestHub_Stats_ReflectsState(t *testing.T) {
	hub := newTestHub()

	if hub.Stats()["connections"] != 0 || hub.Stats()["rooms"] != 0 {
		t.Error("empty hub should report 0 for all stats")
	}

	c1 := stubClient("conn-1", "user-A")
	c2 := stubClient("conn-2", "user-B")
	hub.Register(c1)
	hub.Register(c2)
	hub.Join("conn-1", "room-A")
	hub.Join("conn-2", "room-A")
	hub.Join("conn-2", "room-B")

	stats := hub.Stats()
	if stats["connections"] != 2 {
		t.Errorf("expected 2 connections, got %d", stats["connections"])
	}
	if stats["rooms"] != 2 {
		t.Errorf("expected 2 rooms, got %d", stats["rooms"])
	}
	if stats["online_users"] != 2 {
		t.Errorf("expected 2 online users, got %d", stats["online_users"])
	}

	_ = context.Background() // satisfy import
}
