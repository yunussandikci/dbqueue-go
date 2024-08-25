package types

import (
	"github.com/yunussandikci/dbqueue-go/dbqueue/common"
	"time"
)

type Message struct {
	Payload         []byte
	Priority        uint32
	DeduplicationID *string
	VisibleAfter    *int64
}

type ReceivedMessage struct {
	Message
	ID        uint
	Retrieval *int32
	CreatedAt *int64
}

type ReceiveMessageOptions struct {
	MaxNumberOfMessages *int
	VisibilityTimeout   *time.Duration
	WaitTime            *time.Duration
}

func (r *ReceiveMessageOptions) Defaults() *ReceiveMessageOptions {
	if r.MaxNumberOfMessages == nil {
		r.MaxNumberOfMessages = common.Ptr(0)
	}
	if r.VisibilityTimeout == nil {
		r.VisibilityTimeout = common.Ptr(30 * time.Second)
	}
	if r.WaitTime == nil {
		r.WaitTime = common.Ptr(1 * time.Second)
	}
	return r
}
