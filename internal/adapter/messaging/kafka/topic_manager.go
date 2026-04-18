package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type TopicManager struct {
	admin *kadm.Client
}

func NewTopicManager(client *kgo.Client) *TopicManager {
	return &TopicManager{admin: kadm.NewClient(client)}
}

// SyncPartitions ensures every topic has at least `targetCount` partitions.
// Creates the topic if missing, expands partition count if below target.
// Kafka does not allow reducing partitions — if current count >= target it is left as-is.
func (tm *TopicManager) SyncPartitions(ctx context.Context, topics []string, targetCount int32) error {
	if targetCount <= 0 {
		return fmt.Errorf("targetCount must be positive, got %d", targetCount)
	}

	existing, err := tm.admin.ListTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("listing topics: %w", err)
	}

	var toCreate []string
	var toExpand []string

	for _, topic := range topics {
		detail, found := existing[topic]
		if !found || detail.Err != nil {
			toCreate = append(toCreate, topic)
			continue
		}
		if int32(len(detail.Partitions)) < targetCount {
			toExpand = append(toExpand, topic)
		}
	}

	if len(toCreate) > 0 {
		results, err := tm.admin.CreateTopics(ctx, targetCount, 1, nil, toCreate...)
		if err != nil {
			return fmt.Errorf("creating topics: %w", err)
		}
		for _, r := range results.Sorted() {
			if r.Err != nil {
				zap.S().Warnw("failed to create topic", "topic", r.Topic, "error", r.Err)
			} else {
				zap.S().Infow("topic created", "topic", r.Topic, "partitions", targetCount)
			}
		}
	}

	if len(toExpand) > 0 {
		// UpdatePartitions sets the new TOTAL partition count (not a delta)
		results, err := tm.admin.UpdatePartitions(ctx, int(targetCount), toExpand...)
		if err != nil {
			return fmt.Errorf("expanding partitions: %w", err)
		}
		for _, r := range results.Sorted() {
			if r.Err != nil {
				zap.S().Warnw("failed to expand partitions", "topic", r.Topic, "error", r.Err)
			} else {
				zap.S().Infow("partitions expanded", "topic", r.Topic, "new_count", targetCount)
			}
		}
	}

	return nil
}

// OnNewPair must be called after a new trading pair is persisted to the database.
// newTotalPairCount is the updated total number of active trading pairs.
func (tm *TopicManager) OnNewPair(ctx context.Context, newTotalPairCount int32) error {
	return tm.SyncPartitions(ctx, CoinhubAllCurrentOrderTopics(), newTotalPairCount)
}
