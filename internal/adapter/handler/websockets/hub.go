package coinhub_ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// Client represents one live WebSocket connection.
type Client struct {
	ID     string // unique per connection (e.g. uuid)
	UserID string // authenticated user identity, the userID is the entities::User::UserID
	conn   *websocket.Conn
	rooms  map[string]bool // rooms this client has joined
	mu     sync.Mutex
}

func (c *Client) GetConn() *websocket.Conn {
	return c.conn
}

// Send writes a message to this client. Non-blocking: drops on error
// (the read loop will detect the dead connection and clean up).
func (c *Client) Send(ctx context.Context, msg Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := wsjson.Write(ctx, c.conn, msg); err != nil {
		slog.Warn("send failed", "clientID", c.ID, "err", err)
	}
}

// Hub manages all active clients, rooms, and presence.
// All exported methods are safe for concurrent use.
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client            // connID → client
	rooms   map[string]map[string]*Client // room → connID → client
	// presence: userID → set of connIDs (one user, many tabs)
	presence map[string]map[string]bool
}

func New() *Hub {
	return &Hub{
		clients:  make(map[string]*Client),
		rooms:    make(map[string]map[string]*Client),
		presence: make(map[string]map[string]bool),
	}
}

// Register adds a new connection to the hub.
func (h *Hub) Register(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[c.ID] = c

	if _, ok := h.presence[c.UserID]; !ok {
		h.presence[c.UserID] = make(map[string]bool)
	}
	h.presence[c.UserID][c.ID] = true

	slog.Info("client registered", "connID", c.ID, "userID", c.UserID,
		"totalConns", len(h.clients))
}

// Unregister removes a connection and cleans up all its room memberships.
func (h *Hub) Unregister(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	c, ok := h.clients[connID]
	if !ok {
		return
	}

	// remove from every room it joined
	for room := range c.rooms {
		delete(h.rooms[room], connID)
		if len(h.rooms[room]) == 0 {
			delete(h.rooms, room) // prune empty room
		}
	}

	// update presence
	delete(h.presence[c.UserID], connID)
	if len(h.presence[c.UserID]) == 0 {
		delete(h.presence, c.UserID) // user fully offline
		slog.Info("user offline", "userID", c.UserID)
	}

	delete(h.clients, connID)
	slog.Info("client unregistered", "connID", connID, "userID", c.UserID)
}

// Join adds a client to a room.
func (h *Hub) Join(connID, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	c, ok := h.clients[connID]
	if !ok {
		return
	}

	if h.rooms[room] == nil {
		h.rooms[room] = make(map[string]*Client)
	}
	h.rooms[room][connID] = c

	c.mu.Lock()
	c.rooms[room] = true
	c.mu.Unlock()

	slog.Info("client joined room", "connID", connID, "room", room,
		"roomSize", len(h.rooms[room]))
}

// Leave removes a client from a room.
func (h *Hub) Leave(connID, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	c, ok := h.clients[connID]
	if !ok {
		return
	}

	delete(h.rooms[room], connID)
	if len(h.rooms[room]) == 0 {
		delete(h.rooms, room)
	}

	c.mu.Lock()
	delete(c.rooms, room)
	c.mu.Unlock()
}

// BroadcastRoom sends a message to every client in a room.
func (h *Hub) BroadcastRoom(ctx context.Context, room string, msg Message) {
	h.mu.RLock()
	targets := make([]*Client, 0, len(h.rooms[room]))
	for _, c := range h.rooms[room] {
		targets = append(targets, c)
	}
	h.mu.RUnlock()

	// send outside the lock so slow clients don't block the hub
	for _, c := range targets {
		c.Send(ctx, msg)
	}

	slog.Info("broadcast to room", "room", room, "recipients", len(targets),
		"event", msg.Event)
}

// BroadcastUser sends to all active connections of a specific user.
// Useful for "notify this user regardless of which tab they're on".
func (h *Hub) BroadcastUser(ctx context.Context, userID string, msg Message) {
	h.mu.RLock()
	connIDs := make([]string, 0)
	for id := range h.presence[userID] {
		connIDs = append(connIDs, id)
	}
	h.mu.RUnlock()

	for _, id := range connIDs {
		h.mu.RLock()
		c, ok := h.clients[id]
		h.mu.RUnlock()
		if ok {
			c.Send(ctx, msg)
		}
	}
}

// IsOnline returns true if the user has at least one active connection.
func (h *Hub) IsOnline(userID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.presence[userID]) > 0
}

// Stats returns a snapshot for monitoring/health checks.
func (h *Hub) Stats() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return map[string]int{
		"connections":  len(h.clients),
		"rooms":        len(h.rooms),
		"online_users": len(h.presence),
	}
}

// NewClient constructs a Client. Call hub.Register after.
func NewClient(connID, userID string, conn *websocket.Conn) *Client {
	return &Client{
		ID:     connID,
		UserID: userID,
		conn:   conn,
		rooms:  make(map[string]bool),
	}
}

// MarshalJSON is a helper used in tests/logging to pretty-print a message.
func MarshalJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
