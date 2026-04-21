package cache

import "time"

const (
	GMAIL_VERIFY_PREFIX        = "gmail_verify"
	PENDING_TRANSACTION_PREFIX = "pending_transaction"
	RATE_LIMIT_PREFIX          = "rate_limit"
)

const (
	PENDING_TRANSACTION_LIFETIME_DURATION = time.Hour * 24
)
