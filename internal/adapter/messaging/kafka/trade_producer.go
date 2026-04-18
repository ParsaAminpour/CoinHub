package kafka

import (
	"encoding/json"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func (tep *EngineEventProducer) publishTradeStatusEvent(event TradeStatusEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	topic := CoinhubTradeEventDispatcher(event.GetEventHeader().EventType, "")
	zap.S().Infow("the topic in producer", "topic", topic)
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(event.GetPair()), // e.g. "BTC-USDT"
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte(event.GetEventHeader().EventType)},
			{Key: "event_id", Value: []byte(event.GetEventHeader().EventID)},
			{Key: "event_version", Value: []byte(event.GetEventHeader().Version)},
		},
	}
	if err := tep.producer.ProduceSync(tep.ctx, record).FirstErr(); err != nil {
		return err
	}
	return nil
}

func (tep *EngineEventProducer) PublishTradeStatusEvent(event TradeStatusEvent) error {
	if err := tep.publishTradeStatusEvent(event); err != nil {
		return err
	}
	return nil
}

func (tep *EngineEventProducer) PublishTradeStatusEventBatch(events []TradeStatusEvent) error {
	for _, event := range events {
		if err := tep.publishTradeStatusEvent(event); err != nil {
			return err
		}
	}
	return nil
}
