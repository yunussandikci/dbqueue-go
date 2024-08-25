package types

import (
	"context"
	"time"
)

type Queue interface {
	ReceiveMessage(ctx context.Context, fun func(message ReceivedMessage), options ReceiveMessageOptions) error
	SendMessage(ctx context.Context, message *Message) error
	SendMessageBatch(ctx context.Context, messages []*Message) error
	DeleteMessage(ctx context.Context, id uint) error
	DeleteMessageBatch(ctx context.Context, ids []uint) error
	ChangeMessageVisibility(ctx context.Context, id uint, visibilityTimeout time.Duration) error
	ChangeMessageVisibilityBatch(ctx context.Context, ids []uint, visibilityTimeout time.Duration) error
}
