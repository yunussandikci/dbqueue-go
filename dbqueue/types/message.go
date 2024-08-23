package types

import (
	"time"
)

type Message struct {
	Payload         []byte
	Priority        uint32
	DeduplicationID string
	VisibleAfter    int64
}

type ReceivedMessage struct {
	Message
	ID        uint
	Retrieval *int32
	CreatedAt *int64
}

type ReceiveMessageOptions struct {
	MaxNumberOfMessages int
	VisibilityTimeout   time.Duration
	WaitTime            time.Duration
}
