package notification

import "time"

const (
	READ_BUFFER_SIZE  = 1024
	WRITE_BUFFER_SIZE = 1024
	READ_TIME_OUT     = 60 * time.Second
	WRITE_TIME_OUT    = 10 * time.Second
	MAX_MSG_BYTES     = 8_192
)
