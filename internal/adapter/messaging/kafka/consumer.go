package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type OrderEventConsumer struct {
	ctx      context.Context
	consumer *kgo.Client
}

func NewOrderEventConsumer(ctx context.Context, client *kgo.Client) *OrderEventConsumer {
	return &OrderEventConsumer{
		ctx:      ctx,
		consumer: client,
	}
}
