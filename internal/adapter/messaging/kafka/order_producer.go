package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// EventPublisher is the minimal interface the orderbook needs to emit order events.
// *OrderEventProducer satisfies it; tests use a lightweight in-memory mock.
type EventPublisher interface {
	PublishOrderEvent(event OrderEvent) error
	PublishOrderEventBatch(events []OrderEvent) error
}

type OrderEventProducer struct {
	ctx      context.Context
	producer *kgo.Client
}

func NewOrderEventProducer(ctx context.Context, kafkaClient *kgo.Client) *OrderEventProducer {
	return &OrderEventProducer{
		ctx:      ctx,
		producer: kafkaClient,
	}
}

func (oep *OrderEventProducer) publishOrderEvent(event OrderEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	topic := CoinhubEventDispatcher(event.GetEventHeader().EventType, event.GetSymbol())
	zap.S().Infow("the topic in producer", "topic", topic, "pair", event.GetSymbol())
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(fmt.Sprintf("%s-%s", event.GetBaseAsset(), event.GetQuoteAsset())),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(event.GetEventHeader().EventType)},
			{Key: "event_id", Value: []byte(event.GetEventHeader().EventID)},
			{Key: "event_version", Value: []byte(event.GetEventHeader().Version)},
		},
	}
	if err := oep.producer.ProduceSync(oep.ctx, record).FirstErr(); err != nil {
		return err
	}
	return nil
}

func (oep *OrderEventProducer) PublishOrderEvent(event OrderEvent) error {
	if err := oep.publishOrderEvent(event); err != nil {
		return err
	}
	return nil
}

func (oep *OrderEventProducer) PublishOrderEventBatch(events []OrderEvent) error {
	for _, event := range events {
		if err := oep.PublishOrderEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (oep *OrderEventProducer) Close() {
	oep.producer.Close()
}
