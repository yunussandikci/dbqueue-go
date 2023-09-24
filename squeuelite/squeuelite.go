package squeuelite

import (
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

func (s *SQueueLite) getConnection() *gorm.DB {
	return s.db.Table(fmt.Sprintf("queue_%s", s.queue))
}

func (s *SQueueLite) Put(message *Message) error {
	if message.Id == "" {
		message.Id = uuid.NewString()
	}

	return s.getConnection().Clauses(clause.Insert{Modifier: "OR IGNORE"}).Create(message).Error
}

func (s *SQueueLite) Pop() (*Message, error) {
	var messages []Message

	getErr := s.getConnection().Model(&messages).
		Clauses(clause.Returning{}).
		Where("id = (?)",
			s.getConnection().Model(Message{}).Where("available_after < ?", time.Now()).
				Select("id").
				Limit(1)).
		Update("available_after", time.Now().Add(s.requeueDuration)).Error

	if len(messages) > 0 {
		return &messages[0], nil
	}

	return nil, getErr
}

func (s *SQueueLite) Subscribe(pollingInterval time.Duration) <-chan Message {
	subscription := make(chan Message)

	go func() {
		for {
			message, popErr := s.Pop()
			if popErr != nil || message == nil {
				time.Sleep(pollingInterval)
				continue
			}

			subscription <- *message
		}
	}()

	return subscription
}

func (s *SQueueLite) Done(message Message) error {
	return s.getConnection().Delete(&message).Error
}
