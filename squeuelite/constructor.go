package squeuelite

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func New(database string) (*SQueueLite, error) {
	db, openErr := gorm.Open(sqlite.Open(database), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	return &SQueueLite{
		db: db,
	}, nil
}
