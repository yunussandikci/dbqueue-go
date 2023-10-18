package dbqueue

import (
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewSQLite(dsn string) (*DBQueue, error) {
	return New(sqlite.Open(dsn))
}

func NewPostgreSQL(dsn string) (*DBQueue, error) {
	return New(postgres.Open(dsn))
}

func NewMySQL(dsn string) (*DBQueue, error) {
	return New(mysql.Open(dsn))
}

func New(dialector gorm.Dialector) (*DBQueue, error) {
	database, openErr := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if openErr != nil {
		return nil, openErr
	}

	return &DBQueue{
		db: database,
	}, nil
}
