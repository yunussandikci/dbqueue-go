package squeuelite

import (
	"gorm.io/gorm"
	"time"
)

type SQueueLite struct {
	db *gorm.DB
}

type Message struct {
	Id           string `gorm:"primarykey"`
	Payload      []byte
	Retry        uint32
	Priority     uint32
	VisibleAfter time.Time
}

type ReceiveMessageOptions struct {
	MaxNumberOfMessages int
	VisibilityTimeout   time.Duration
	WaitTime            time.Duration
}

func (r *ReceiveMessageOptions) GetMaxNumberOfMessages() int {
	if r.MaxNumberOfMessages != 0 {
		return r.MaxNumberOfMessages
	}

	return 1
}

func (r *ReceiveMessageOptions) GetWaitTime() time.Duration {
	if r.WaitTime != 0 {
		return r.WaitTime
	}

	return time.Second
}

func (r *ReceiveMessageOptions) GetVisibilityTimeout() time.Duration {
	if r.VisibilityTimeout != 0 {
		return r.VisibilityTimeout
	}

	return time.Minute
}
