package dbqueue

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *DBQueue) ReceiveMessage(queue string, fun func(message Message), options ReceiveMessageOptions) error {
	for {
		var messages []Message

		if s.isMySQL() {
			if txErr := s.db.Transaction(func(trx *gorm.DB) error {
				if findErr := trx.Table(queue).
					Clauses(clause.Locking{
						Strength: "UPDATE",
						Options:  "SKIP LOCKED",
					}).
					Where("visible_after < ?", time.Now().Unix()).
					Order("priority desc").
					Limit(options.GetMaxNumberOfMessages()).
					Find(&messages).Error; findErr != nil {
					return findErr
				}

				var ids []string
				for _, item := range messages {
					ids = append(ids, item.ID)
				}

				if updateErr := trx.Table(queue).
					Where("id IN ?", ids).
					Updates(map[string]interface{}{
						"visible_after": time.Now().Add(options.GetVisibilityTimeout()).Unix(),
						"retry":         gorm.Expr("retry + ?", 1)}).Error; updateErr != nil {
					return updateErr
				}

				return nil
			}); txErr != nil {
				return txErr
			}
		} else {
			if queryErr := s.db.Table(queue).
				Model(&messages).
				Clauses(clause.Returning{}).
				Where("id = (?)", s.db.Table(queue).Model(&messages).
					Clauses(clause.Locking{
						Strength: "UPDATE",
						Options:  "SKIP LOCKED",
					}).
					Select("id").
					Where("visible_after < ?", time.Now().Unix()).
					Order("priority desc").
					Limit(options.GetMaxNumberOfMessages())).
				Updates(map[string]interface{}{
					"visible_after": time.Now().Add(options.GetVisibilityTimeout()).Unix(),
					"retry":         gorm.Expr("retry + ?", 1),
				}).Error; queryErr != nil {
				return queryErr
			}
		}

		for _, message := range messages {
			fun(message)
		}

		if len(messages) == 0 {
			time.Sleep(options.GetWaitTime())
		}
	}
}

func (s *DBQueue) SendMessage(queue string, message *Message) error {
	return s.SendMessageBatch(queue, []*Message{message})
}

func (s *DBQueue) SendMessageBatch(queue string, messages []*Message) error {
	for _, message := range messages {
		if message.ID == "" {
			message.ID = uuid.NewString()
		}
		if message.VisibleAfter == 0 {
			message.VisibleAfter = time.Now().Unix()
		}

		message.Retry = 0
	}

	return s.db.Table(queue).Clauses(clause.OnConflict{
		DoNothing: true,
		Columns: []clause.Column{{
			Name: "id",
		}},
	}).Create(messages).Error
}

func (s *DBQueue) DeleteMessage(queue string, messageID string) error {
	return s.DeleteMessageBatch(queue, []string{messageID})
}

func (s *DBQueue) DeleteMessageBatch(queue string, messageIDs []string) error {
	return s.db.Table(queue).Delete(&Message{}, messageIDs).Error
}

func (s *DBQueue) ChangeMessageVisibility(queue string, visibilityTimeout time.Duration, messageID string) error {
	return s.ChangeMessageVisibilityBatch(queue, visibilityTimeout, []string{messageID})
}

func (s *DBQueue) ChangeMessageVisibilityBatch(queue string, visibilityTimeout time.Duration, messageIDs []string) error {
	return s.db.Table(queue).Model(&Message{}).Where("id IN ?", messageIDs).
		Update("visible_after", time.Now().Add(visibilityTimeout).Unix()).Error
}

func (s *DBQueue) CreateQueue(name string) error {
	if migrateErr := s.db.Table(name).AutoMigrate(&Message{}); migrateErr != nil &&
		!strings.Contains(migrateErr.Error(), "already exist") {
		return migrateErr
	}

	return nil
}

func (s *DBQueue) DeleteQueue(name string) error {
	return s.db.Exec(fmt.Sprintf("DROP TABLE %s;", name)).Error
}

func (s *DBQueue) PurgeQueue(queue string) error {
	return s.db.Table(queue).Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&Message{}).Error
}
