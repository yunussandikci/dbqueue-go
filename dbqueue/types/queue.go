package types

import "time"

type Queue interface {
	ReceiveMessage(fun func(message ReceivedMessage), options ReceiveMessageOptions) error
	SendMessage(message *Message) error
	SendMessageBatch(messages []*Message) error
	DeleteMessage(id uint) error
	DeleteMessageBatch(ids []uint) error
	ChangeMessageVisibility(id uint, visibilityTimeout time.Duration) error
	ChangeMessageVisibilityBatch(ids []uint, visibilityTimeout time.Duration) error
}
