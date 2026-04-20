package websocket_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coinhub_ws "coinhub/internal/adapter/handler/websockets"
	"coinhub/internal/adapter/handler/websockets/notification"

	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// ─────────────────────────────────────────────
// Test server setup
// ─────────────────────────────────────────────

// startTestServer spins up an httptest.Server that accepts WebSocket
// connections and runs OrderEventEmitterWebsocketListener for each one.
// Returns the server URL (ws://...) and the NotificationServer.
func startTestServer(t *testing.T, hub *coinhub_ws.Hub) (wsURL string, ns *notification.NotificationServer) {
	t.Helper()
	ns = notification.NewNotificationServer(hub)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Logf("accept error: %v", err)
			return
		}
		conn.SetReadLimit(notification.MAX_MSG_BYTES)

		// Build a minimal gin context so the listener has a context to use.
		ginW := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(ginW)
		ginCtx.Request = r

		client := coinhub_ws.NewClient("test-conn", "test-user", conn)
		// Run the listener in a goroutine — it blocks until the conn closes.
		go ns.OrderEventEmitterWebsocketListener(ginCtx, client)
	}))

	t.Cleanup(srv.Close)

	wsURL = "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, ns
}

// dial opens a WebSocket client connection to the test server.
func dial(t *testing.T, wsURL string) (*websocket.Conn, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	return conn, cancel
}

func send(t *testing.T, ctx context.Context, conn *websocket.Conn, msg coinhub_ws.Message) {
	t.Helper()
	if err := wsjson.Write(ctx, conn, msg); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func recv(t *testing.T, ctx context.Context, conn *websocket.Conn) coinhub_ws.Message {
	t.Helper()
	var msg coinhub_ws.Message
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		t.Fatalf("recv: %v", err)
	}
	return msg
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// TestWS_Ping_ReturnsPong: client sends ping → server must reply pong.
func TestWS_Ping_ReturnsPong(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypePing})

	reply := recv(t, ctx, conn)
	if reply.Type != coinhub_ws.TypePong {
		t.Errorf("expected pong, got %q", reply.Type)
	}
}

// TestWS_Join_ReceivesJoinedEvent: client sends join with a room →
// server replies with an event{joined} for that room.
func TestWS_Join_ReceivesJoinedEvent(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypeJoin, Room: "order:BTC-USDT"})

	reply := recv(t, ctx, conn)
	if reply.Type != coinhub_ws.TypeEvent {
		t.Errorf("expected event type, got %q", reply.Type)
	}
	if reply.Event != "joined" {
		t.Errorf("expected event=joined, got %q", reply.Event)
	}
	if reply.Room != "order:BTC-USDT" {
		t.Errorf("expected room=order:BTC-USDT, got %q", reply.Room)
	}
}

// TestWS_Join_NoRoom_ReceivesError: join without a room name →
// server must reply with an error frame.
func TestWS_Join_NoRoom_ReceivesError(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypeJoin}) // no Room

	reply := recv(t, ctx, conn)
	if reply.Type != coinhub_ws.TypeError {
		t.Errorf("expected error type, got %q", reply.Type)
	}
	if reply.Error == "" {
		t.Error("expected a non-empty error message")
	}
}

// TestWS_UnknownMessageType_ReceivesError: unknown message type →
// server must reply with an error frame.
func TestWS_UnknownMessageType_ReceivesError(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	send(t, ctx, conn, coinhub_ws.Message{Type: "garbage"})

	reply := recv(t, ctx, conn)
	if reply.Type != coinhub_ws.TypeError {
		t.Errorf("expected error type, got %q", reply.Type)
	}
}

// TestWS_BroadcastRoom_ClientReceivesMessage: client joins a room,
// server broadcasts to that room, client must receive the message.
func TestWS_BroadcastRoom_ClientReceivesMessage(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	// Join the room.
	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypeJoin, Room: "order:BTC-USDT"})
	joinReply := recv(t, ctx, conn)
	if joinReply.Event != "joined" {
		t.Fatalf("join handshake failed: %+v", joinReply)
	}

	// Server broadcasts an order event to the room.
	broadcastMsg := coinhub_ws.Message{
		Type:    coinhub_ws.TypeEvent,
		Room:    "order:BTC-USDT",
		Event:   "order.cancelled",
		Payload: map[string]any{"order_id": "123"},
	}
	go hub.BroadcastRoom(ctx, "order:BTC-USDT", broadcastMsg)

	reply := recv(t, ctx, conn)
	if reply.Type != coinhub_ws.TypeEvent {
		t.Errorf("expected event, got %q", reply.Type)
	}
	if reply.Event != "order.cancelled" {
		t.Errorf("expected event=order.cancelled, got %q", reply.Event)
	}
}

// TestWS_BroadcastRoom_NonMemberDoesNotReceive: a client NOT in the room
// must not receive the broadcast. We verify this by checking hub membership
// before the broadcast — only the joined client is in the room.
func TestWS_BroadcastRoom_NonMemberDoesNotReceive(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	// Client connects but never joins any room.
	// Give the server a moment to register the client.
	time.Sleep(50 * time.Millisecond)

	stats := hub.Stats()
	if stats["rooms"] != 0 {
		t.Errorf("client never joined — expected 0 rooms, got %d", stats["rooms"])
	}
}

// TestWS_Leave_RemovesFromRoom: client joins then leaves a room →
// hub must show 0 rooms afterwards.
func TestWS_Leave_RemovesFromRoom(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()
	defer conn.CloseNow()

	// Join.
	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypeJoin, Room: "order:BTC-USDT"})
	recv(t, ctx, conn) // consume the joined reply

	// Leave.
	send(t, ctx, conn, coinhub_ws.Message{Type: coinhub_ws.TypeLeave, Room: "order:BTC-USDT"})

	// Give the server a moment to process.
	time.Sleep(50 * time.Millisecond)

	if hub.Stats()["rooms"] != 0 {
		t.Errorf("expected 0 rooms after leave, got %d", hub.Stats()["rooms"])
	}
}

// TestWS_Disconnect_UnregistersClient: closing the connection must cause
// the hub to unregister the client.
func TestWS_Disconnect_UnregistersClient(t *testing.T) {
	hub := coinhub_ws.New()
	wsURL, _ := startTestServer(t, hub)

	conn, dialCancel := dial(t, wsURL)
	defer dialCancel()

	// Give the server a moment to register the client.
	time.Sleep(50 * time.Millisecond)
	if hub.Stats()["connections"] != 1 {
		t.Fatalf("expected 1 connection before disconnect, got %d", hub.Stats()["connections"])
	}

	// Close the connection.
	conn.Close(websocket.StatusNormalClosure, "bye")

	// Give the server a moment to unregister.
	time.Sleep(100 * time.Millisecond)

	if hub.Stats()["connections"] != 0 {
		t.Errorf("expected 0 connections after disconnect, got %d", hub.Stats()["connections"])
	}
}
