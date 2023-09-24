package squeuelite

import (
	"gorm.io/gorm"
	"time"
)

type SQueueLite struct {
	db              *gorm.DB
	requeueDuration time.Duration
	queue           string
}

type Config struct {
	Database        string
	Queue           string
	RequeueDuration time.Duration
}

type Message struct {
	Id             string `gorm:"primarykey"`
	Payload        []byte
	AvailableAfter time.Time
	Priority       uint8
}
