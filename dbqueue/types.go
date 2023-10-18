package dbqueue

import (
	"time"

	"gorm.io/gorm"
)

type DBQueue struct {
	db *gorm.DB
}

func (s *DBQueue) isMySQL() bool {
	return s.db.Config.Dialector.Name() == "mysql"
}

type Message struct {
	ID              uint   `gorm:"primarykey"`
	DeduplicationID string `gorm:"unique"`
	Payload         []byte
	Priority        uint32
	Retrieval       int32
	VisibleAfter    int64
	CreatedAt       int64
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
