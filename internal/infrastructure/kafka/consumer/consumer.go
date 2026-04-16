package consumer

import (
	"coinhub/internal/adapter/messaging/kafka"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, event kafka.OrderStatusEvent, record *kgo.Record) error

type Runner struct {
	client        *kgo.Client
	handler       MessageHandler
	name          string
	maxRetries    int
	retryBackoff  time.Duration
	dlqTopic      string
	dlqProducer   *kgo.Client
	processed     uint64
	retried       uint64
	dlqMessages   uint64
	failedRecords uint64
}

func NewRunner(client *kgo.Client, handler MessageHandler, name string, maxRetries int, retryBackoff time.Duration) *Runner {
	return &Runner{
		client:       client,
		handler:      handler,
		name:         name,
		maxRetries:   maxRetries,
		retryBackoff: retryBackoff,
	}
}

func (r *Runner) WithDLQ(dlqProducer *kgo.Client, dlqTopic string) *Runner {
	r.dlqProducer = dlqProducer
	r.dlqTopic = dlqTopic
	return r
}

func (r *Runner) Run(ctx context.Context) error {
	zap.S().Infow("kafka consumer runner started",
		"consumer_name", r.name,
		"retry_backoff", r.retryBackoff.String(),
		"max_retries", r.maxRetries,
		"dlq_enabled", r.dlqProducer != nil && r.dlqTopic != "",
	)
	defer r.logStats()

	for {
		select {
		case <-ctx.Done():
			zap.S().Infow("kafka consumer shutting down", "consumer_name", r.name)
			return nil
		default:
		}

		fetches := r.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, ferr := range errs {
				zap.S().Errorw("kafka poll error", "consumer_name", r.name, "error", ferr)
			}
		}

		iter := fetches.RecordIter()
		var toCommit []*kgo.Record
		for !iter.Done() {
			record := iter.Next()
			if record == nil {
				continue
			}
			if err := r.processRecord(ctx, record); err != nil {
				r.failedRecords++
				zap.S().Errorw("record processing failed",
					"consumer_name", r.name,
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"error", err,
				)
				continue
			}
			toCommit = append(toCommit, record)
			r.processed++
		}
		if len(toCommit) > 0 {
			if err := r.client.CommitRecords(ctx, toCommit...); err != nil {
				r.failedRecords += uint64(len(toCommit))
				zap.S().Errorw("batch offset commit failed",
					"consumer_name", r.name,
					"error", err,
				)
			}
		}
	}
}

func (r *Runner) processRecord(ctx context.Context, record *kgo.Record) error {
	var event kafka.OrderStatusEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return r.sendToDLQ(ctx, record, errors.New("invalid payload"))
	}
	if event.EventHeader.EventID == "" {
		return r.sendToDLQ(ctx, record, errors.New("missing event_id"))
	}

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := r.handler(ctx, event, record)
		if err == nil {
			return nil
		}
		if attempt == r.maxRetries {
			return r.sendToDLQ(ctx, record, err)
		}
		r.retried++
		zap.S().Warnw("handler failed, retrying",
			"consumer_name", r.name,
			"event_id", event.EventID,
			"attempt", attempt+1,
			"max_retries", r.maxRetries,
			"error", err,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.retryBackoff):
		}
	}
	return nil
}

func (r *Runner) sendToDLQ(ctx context.Context, record *kgo.Record, failure error) error {
	if r.dlqProducer == nil || r.dlqTopic == "" {
		return failure
	}
	dlqRecord := &kgo.Record{
		Topic: r.dlqTopic,
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers, []kgo.RecordHeader{
			{Key: "x_failure_reason", Value: []byte(failure.Error())},
			{Key: "x_source_topic", Value: []byte(record.Topic)},
		}...),
	}
	if err := r.dlqProducer.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		return errors.Join(failure, err)
	}
	r.dlqMessages++
	zap.S().Errorw("record published to dlq",
		"consumer_name", r.name,
		"dlq_topic", r.dlqTopic,
		"source_topic", record.Topic,
		"partition", record.Partition,
		"offset", record.Offset,
		"error", failure,
	)
	return nil
}

func (r *Runner) logStats() {
	zap.S().Infow("kafka consumer runner stopped",
		"consumer_name", r.name,
		"processed_records", r.processed,
		"retried_records", r.retried,
		"failed_records", r.failedRecords,
		"dlq_messages", r.dlqMessages,
	)
}
