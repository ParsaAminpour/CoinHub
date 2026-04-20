package notification

import (
	coinhub_ws "coinhub/internal/adapter/handler/websockets"
	"context"
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type NotificationServer struct {
	Hub *coinhub_ws.Hub
}

func NewNotificationServer(hub *coinhub_ws.Hub) *NotificationServer {
	return &NotificationServer{
		Hub: hub,
	}
}

func (ns *NotificationServer) OrderEventEmitterWebsocketListener(c *gin.Context, client *coinhub_ws.Client) error {
	// register client to its room
	defer ns.Hub.Unregister(client.ID)
	ns.Hub.Register(client)

	// constantly listening to the associated room
	for {
		readCtx, cancel := context.WithTimeout(c, READ_TIME_OUT)

		var msg coinhub_ws.Message
		err := wsjson.Read(readCtx, client.GetConn(), &msg) // nhooyr handles ping/pong internally
		cancel()

		if err != nil {
			// Distinguish clean close from unexpected error.
			var closeErr websocket.CloseError
			if errors.As(err, &closeErr) {
				zap.S().Infow("ws closed cleanly", "connID", client.ID,
					"code", closeErr.Code, "reason", closeErr.Reason)
			} else {
				zap.S().Warnw("read error", "connID", client.ID, "err", err)
			}
			return fmt.Errorf("websocket read error: %w", err)
		}

		ns.handleMessage(c, client, msg)
	}
}
