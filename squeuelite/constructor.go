package squeuelite

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"time"
)

var defaultRequeueDuration = time.Minute
var defaultQueue = "default"

func New(config Config) (*SQueueLite, error) {
	db, openErr := gorm.Open(sqlite.Open(config.Database), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	if config.RequeueDuration == 0 {
		config.RequeueDuration = defaultRequeueDuration
	}

	if len(config.Queue) == 0 {
		config.Queue = defaultQueue
	}

	instance := &SQueueLite{
		db:              db,
		requeueDuration: config.RequeueDuration,
		queue:           config.Queue,
	}

	migrateErr := instance.getConnection().AutoMigrate(&Message{})
	if migrateErr != nil && !strings.Contains(migrateErr.Error(), "already exist") {
		return nil, migrateErr
	}

	return instance, nil
}
