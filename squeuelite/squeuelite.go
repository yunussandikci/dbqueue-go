package squeuelite

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

func (s *SQueueLite) CreateQueue(name string) error {
	migrateErr := s.db.Table(name).AutoMigrate(&Message{})
	if migrateErr != nil && !strings.Contains(migrateErr.Error(), "already exist") {
		return migrateErr
	}

	return nil
}

func (s *SQueueLite) SendMessage(queue string, message *Message) error {
	return s.SendMessageBatch(queue, []*Message{message})
}

func (s *SQueueLite) SendMessageBatch(queue string, messages []*Message) error {
	for _, message := range messages {
		if message.Id == "" {
			message.Id = uuid.NewString()
		}
		if message.VisibleAfter.IsZero() {
			message.VisibleAfter = time.Now()
		}
	}

	return s.db.Table(queue).Clauses(clause.Insert{Modifier: "OR IGNORE"}).Create(messages).Error
}

func (s *SQueueLite) DeleteMessage(queue string, messageId string) error {
	return s.DeleteMessageBatch(queue, []string{messageId})
}

func (s *SQueueLite) DeleteMessageBatch(queue string, messageIds []string) error {
	return s.db.Table(queue).Delete(&Message{}, messageIds).Error
}

func (s *SQueueLite) ReceiveMessage(queue string, f func(message Message), options ReceiveMessageOptions) error {
	for {
		var messages []Message

		getErr := s.db.Table(queue).
			Model(&messages).
			Clauses(clause.Returning{}).
			Where("id = (?)",
				s.db.Table(queue).
					Where("visible_after < ?", time.Now()).
					Select("id").
					Order("priority desc").
					Limit(options.GetMaxNumberOfMessages())).
			Updates(map[string]interface{}{
				"visible_after": time.Now().Add(options.GetVisibilityTimeout()),
				"retry":         gorm.Expr("retry + ?", 1),
			}).Error

		if getErr != nil {
			return getErr
		}

		if len(messages) == 0 {
			time.Sleep(options.GetWaitTime())
			continue
		}

		for _, message := range messages {
			f(message)
		}
	}
}

func (s *SQueueLite) PurgeQueue(queue string) error {
	return s.db.Table(queue).Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&Message{}).Error
}

func (s *SQueueLite) ChangeMessageVisibility(queue string, visibilityTimeout time.Duration, messageId string) error {
	return s.db.Table(queue).Model(&Message{}).Where("id = ?", messageId).
		Update("visible_after", time.Now().Add(visibilityTimeout)).Error
}
