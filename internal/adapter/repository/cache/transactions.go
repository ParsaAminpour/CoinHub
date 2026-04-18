package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type PendingTransactionsCache struct {
	ctx   context.Context
	redis *redis.Client
	ttl   time.Duration
}

func NewPendingTransactionsCache(ctx context.Context, redis *redis.Client, ttl time.Duration) *PendingTransactionsCache {
	return &PendingTransactionsCache{
		ctx:   ctx,
		redis: redis,
		ttl:   ttl,
	}
}

func (c *PendingTransactionsCache) SetPendingTransaction(ctx context.Context, trxHash string) error {
	return c.redis.Set(ctx, fmt.Sprintf("%s:%s", PENDING_TRANSACTION_PREFIX, trxHash), trxHash, c.ttl).Err()
}

func (c *PendingTransactionsCache) GetPendingTransaction(ctx context.Context, trxHash string) (string, error) {
	return c.redis.Get(ctx, fmt.Sprintf("%s:%s", PENDING_TRANSACTION_PREFIX, trxHash)).Result()
}

func (c *PendingTransactionsCache) DeletePendingTransaction(ctx context.Context, trxHash string) error {
	return c.redis.Del(ctx, fmt.Sprintf("%s:%s", PENDING_TRANSACTION_PREFIX, trxHash)).Err()
}
