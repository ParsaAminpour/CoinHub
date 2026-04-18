package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// EventPublisher is the minimal interface the orderbook needs to emit order events.
// *EngineEventProducer satisfies it; tests use a lightweight in-memory mock.
type EventPublisher interface {
	PublishOrderEvent(event OrderEvent) error
	PublishOrderEventBatch(events []OrderEvent) error
	PublishTradeStatusEvent(event TradeStatusEvent) error
	PublishTradeStatusEventBatch(events []TradeStatusEvent) error
}

// For both order and trade events.
type EngineEventProducer struct {
	ctx      context.Context
	producer *kgo.Client
}

func NewEngineEventProducer(ctx context.Context, kafkaClient *kgo.Client) *EngineEventProducer {
	return &EngineEventProducer{
		ctx:      ctx,
		producer: kafkaClient,
	}
}

func (tep *EngineEventProducer) Close() {
	tep.producer.Close()
}
