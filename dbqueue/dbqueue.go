package dbqueue

import (
	"errors"
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
			trx := s.db.Begin()

			if findErr := trx.Table(queue).
				Clauses(clause.Locking{
					Strength: "UPDATE",
					Options:  "SKIP LOCKED",
				}).
				Where("visible_after < ?", time.Now().Unix()).
				Order("priority desc").
				Order("id asc").
				Limit(options.GetMaxNumberOfMessages()).
				Find(&messages).Error; findErr != nil {
				return errors.Join(findErr, trx.Rollback().Error)
			}

			visibleAfter := time.Now().Add(options.GetVisibilityTimeout()).Unix()

			var ids []uint
			for key, value := range messages {
				messages[key].VisibleAfter = visibleAfter
				messages[key].Retry++
				ids = append(ids, value.ID)
			}

			if updateErr := trx.Table(queue).Model(&messages).
				Where("id IN ?", ids).
				Updates(map[string]interface{}{
					"visible_after": visibleAfter,
					"retry":         gorm.Expr("retry + ?", 1)}).Error; updateErr != nil {

				return errors.Join(updateErr, trx.Rollback().Error)
			}

			if commitErr := trx.Commit().Error; commitErr != nil {
				return commitErr
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
					Order("id asc").
					Limit(options.GetMaxNumberOfMessages())).
				Updates(map[string]interface{}{
					"retry":         gorm.Expr("retry + ?", 1),
					"visible_after": time.Now().Add(options.GetVisibilityTimeout()).Unix(),
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
		if message.DeduplicationID == "" {
			message.DeduplicationID = uuid.NewString()
		}

		if message.VisibleAfter == 0 {
			message.VisibleAfter = time.Now().Unix()
		}
	}

	return s.db.Table(queue).Clauses(clause.OnConflict{DoNothing: true}).Create(messages).Error
}

func (s *DBQueue) DeleteMessage(queue string, id uint) error {
	return s.DeleteMessageBatch(queue, []uint{id})
}

func (s *DBQueue) DeleteMessageBatch(queue string, ids []uint) error {
	return s.db.Table(queue).Delete(&Message{}, ids).Error
}

func (s *DBQueue) ChangeMessageVisibility(queue string, visibilityTimeout time.Duration, id uint) error {
	return s.ChangeMessageVisibilityBatch(queue, visibilityTimeout, []uint{id})
}

func (s *DBQueue) ChangeMessageVisibilityBatch(queue string, visibilityTimeout time.Duration, ids []uint) error {
	return s.db.Table(queue).Model(&Message{}).Where("id IN ?", ids).
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
	return s.db.Exec(fmt.Sprintf(`DROP TABLE %s;`, name)).Error
}

func (s *DBQueue) PurgeQueue(queue string) error {
	return s.db.Table(queue).Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&Message{}).Error
}
