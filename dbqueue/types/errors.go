package types

import "errors"

var (
	ErrQueueNotFound        = errors.New("queue not found")
	ErrDatabaseNotSupported = errors.New("database not supported")
)
