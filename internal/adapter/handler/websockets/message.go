package coinhub_ws

// MessageType categorises every frame on the wire.
type MessageType string

const (
	TypeJoin  MessageType = "join"  // client → server: join a room
	TypeLeave MessageType = "leave" // client → server: leave a room
	TypeEvent MessageType = "event" // server → client: domain event
	TypePing  MessageType = "ping"  // client → server: keepalive
	TypePong  MessageType = "pong"  // server → client: keepalive reply
	TypeError MessageType = "error" // server → client: error notice
)

// Message is the single envelope used for every frame.
// Keeping one type makes handler code uniform and easy to log.
type Message struct {
	Type    MessageType `json:"type"`
	Room    string      `json:"room,omitempty"`
	Event   string      `json:"event,omitempty"`
	Payload any         `json:"payload,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// NewMessage creates a new Message with the given type, event, and payload.
func NewMessage(msgType MessageType, event string, payload any) Message {
	return Message{
		Type:    msgType,
		Event:   event,
		Payload: payload,
	}
}
