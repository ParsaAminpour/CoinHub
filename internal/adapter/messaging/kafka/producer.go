package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

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

func (oep *OrderEventProducer) PublishOrderEvent(event OrderEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	record := &kgo.Record{
		Topic: CoinHubFilledOrderEventTopic(event.getSymbol()),
		Key:   []byte(fmt.Sprintf("%s", event.getOrderID())),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(event.getEventHeader().EventType)},
			{Key: "event_id", Value: []byte(event.getEventHeader().EventID)},
		},
	}
	if err := oep.producer.ProduceSync(oep.ctx, record).FirstErr(); err != nil {
		return err
	}
	return nil
}
