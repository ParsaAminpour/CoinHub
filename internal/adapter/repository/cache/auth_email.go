package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type AuthGmailCache struct {
	ctx   context.Context
	redis *redis.Client
	ttl   time.Duration
}

func NewAuthGmailCache(ctx context.Context, redis *redis.Client, ttl time.Duration) *AuthGmailCache {
	return &AuthGmailCache{
		ctx:   ctx,
		redis: redis,
		ttl:   ttl,
	}
}

func (c AuthGmailCache) SetGmailVerificationCode(ctx context.Context, redis *redis.Client, ttl time.Duration, gmail, username, code string) error {
	return c.redis.Set(
		ctx,
		fmt.Sprintf("%s:%s:%s", GMAIL_VERIFY_PREFIX, gmail, username),
		code,
		c.ttl).Err()
}

func (c AuthGmailCache) GetGmailVerificationCode(ctx context.Context, redis *redis.Client, gmail, username string) (string, error) {
	return c.redis.Get(ctx, fmt.Sprintf("%s:%s:%s", GMAIL_VERIFY_PREFIX, gmail, username)).Result()
}

func (c AuthGmailCache) DeleteGmailVerificationCode(ctx context.Context, redis *redis.Client, gmail, username string) error {
	return c.redis.Del(ctx, fmt.Sprintf("%s:%s:%s", GMAIL_VERIFY_PREFIX, gmail, username)).Err()
}

func (c AuthGmailCache) GetGmailVerificationCodeTimeLeft(ctx context.Context, redis *redis.Client, gmail, username string) (time.Duration, error) {
	ttl, err := c.redis.TTL(ctx, fmt.Sprintf("%s:%s:%s", GMAIL_VERIFY_PREFIX, gmail, username)).Result()
	if err != nil {
		return 0, err
	}
	return ttl, nil
}
