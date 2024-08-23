package types

import "errors"

var (
	ErrQueueNotFound      = errors.New("queue not found")
	ErrEngineNotSupported = errors.New("engine not supported")
)
