package main

import (
	"fmt"
	"time"

	"github.com/yunussandikci/dbqueue-go/dbqueue"
)

func main() {
	// dbQueue, queueErr := dbqueue.NewPostgreSQL("host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable")
	// dbQueue, queueErr := dbqueue.NewSQLite("hello.db")
	dbQueue, queueErr := dbqueue.NewMySQL("root:root@tcp(127.0.0.1:3306)/mydb")
	if queueErr != nil {
		panic(queueErr)
	}

	// Create Queue
	if createQueueErr := dbQueue.CreateQueue("my-queue"); createQueueErr != nil {
		panic(createQueueErr)
	}

	// Send items into Queue
	for i := 1; i < 100; i++ {
		if putErr := dbQueue.SendMessage("my-queue", &dbqueue.Message{
			Payload: []byte(fmt.Sprintf("message-%d", i)),
		}); putErr != nil {
			panic(putErr)
		}
	}

	go func() {
		if receiverErr := dbQueue.ReceiveMessage("my-queue", func(message dbqueue.Message) {
			fmt.Printf("A Payload:%s Priority:%d Retry:%d\n", string(message.Payload), message.Priority, message.Retry)

			// Delete Messages
			if deleteErr := dbQueue.DeleteMessage("my-queue", message.ID); deleteErr != nil {
				panic(deleteErr)
			}
		}, dbqueue.ReceiveMessageOptions{
			VisibilityTimeout: time.Minute * 10,
			WaitTime:          0,
		}); receiverErr != nil {
			panic(receiverErr)
		}
	}()
	go func() {
		if receiverErr := dbQueue.ReceiveMessage("my-queue", func(message dbqueue.Message) {
			fmt.Printf("B Payload:%s Priority:%d Retry:%d\n", string(message.Payload), message.Priority, message.Retry)

			// Delete Messages
			if deleteErr := dbQueue.DeleteMessage("my-queue", message.ID); deleteErr != nil {
				panic(deleteErr)
			}
		}, dbqueue.ReceiveMessageOptions{
			VisibilityTimeout: time.Minute * 10,
			WaitTime:          0,
		}); receiverErr != nil {
			panic(receiverErr)
		}
	}()

	select {}
}
