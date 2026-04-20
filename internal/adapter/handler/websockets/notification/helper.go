package notification

import (
	coinhub_ws "coinhub/internal/adapter/handler/websockets"
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// handleMessage dispatches a client frame to the right action.
func (s *NotificationServer) handleMessage(ctx context.Context, c *coinhub_ws.Client, msg coinhub_ws.Message) {
	switch msg.Type {
	case coinhub_ws.TypeJoin:
		if msg.Room == "" {
			s.sendError(ctx, c, "join requires a room name")
			return
		}
		s.Hub.Join(c.ID, msg.Room)
		c.Send(ctx, coinhub_ws.Message{
			Type:  coinhub_ws.TypeEvent,
			Room:  msg.Room,
			Event: "joined",
		})

	case coinhub_ws.TypeLeave:
		s.Hub.Leave(c.ID, msg.Room)

	case coinhub_ws.TypePing:
		c.Send(ctx, coinhub_ws.Message{Type: coinhub_ws.TypePong})

	default:
		slog.Warn("unknown message type", "type", msg.Type, "connID", c.ID)
		s.sendError(ctx, c, "unknown message type: "+string(msg.Type))
	}
}

func (s *NotificationServer) sendError(ctx context.Context, c *coinhub_ws.Client, reason string) {
	c.Send(ctx, coinhub_ws.Message{Type: coinhub_ws.TypeError, Error: reason})
}

// handleEmit is an HTTP trigger so you can test broadcasts via curl.
// POST /emit?room=lobby  {"event":"order.updated","payload":{...}}
func (s *NotificationServer) handleEmit(c *gin.Context) {
	if c.Request.Method != http.MethodPost {
		c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "POST only"})
		return
	}
	room := c.Query("room")
	if room == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing room"})
		return
	}

	var msg coinhub_ws.Message
	if err := c.ShouldBindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad body"})
		return
	}
	msg.Type = coinhub_ws.TypeEvent
	msg.Room = room

	s.Hub.BroadcastRoom(c.Request.Context(), room, msg)
	c.Status(http.StatusNoContent)
}
