package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RateLimiterCache struct {
	cctx  context.Context
	redis *redis.Client
	ttl   time.Duration
}

func NewRateLimiterCache(ctx context.Context, redisClient *redis.Client, ttl time.Duration) *RateLimiterCache {
	return &RateLimiterCache{
		cctx:  ctx,
		redis: redisClient,
		ttl:   ttl,
	}
}

// Allow increments the request count for the given key and returns whether the
// request is within the limit. Uses a fixed window keyed by the current window
// start (floor of now / ttl). The TTL is set only on first increment so the
// window expires naturally without a separate cleanup job.
func (r *RateLimiterCache) Allow(ctx context.Context, ip string, maxRequests int) (bool, error) {
	window := time.Now().Truncate(r.ttl).Unix()
	key := fmt.Sprintf("%s:%s:%d", RATE_LIMIT_PREFIX, ip, window)

	count, err := r.redis.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}

	// Set TTL only on the first increment so we don't keep pushing the window out.
	if count == 1 {
		r.redis.Expire(ctx, key, r.ttl)
	}

	return count <= int64(maxRequests), nil
}
