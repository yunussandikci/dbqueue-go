package dbqueue

import (
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewSQLite(dsn string) (*DBQueue, error) {
	database, openErr := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	return &DBQueue{
		db:             database,
		useTransaction: false,
	}, nil
}

func NewPostgreSQL(dsn string) (*DBQueue, error) {
	database, openErr := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	return &DBQueue{
		db:             database,
		useTransaction: false,
	}, nil
}

func NewMySQL(dsn string) (*DBQueue, error) {
	database, openErr := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	return &DBQueue{
		db:             database,
		useTransaction: true,
	}, nil
}
