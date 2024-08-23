package types

type Engine interface {
	OpenQueue(name string) (Queue, error)
	CreateQueue(name string) (Queue, error)
	DeleteQueue(name string) error
	PurgeQueue(name string) error
}
