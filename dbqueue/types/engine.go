package types

import "context"

type Engine interface {
	OpenQueue(ctx context.Context, name string) (Queue, error)
	CreateQueue(ctx context.Context, name string) (Queue, error)
	DeleteQueue(ctx context.Context, name string) error
	PurgeQueue(ctx context.Context, name string) error
}
