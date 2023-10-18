package main

import (
	"fmt"
	"time"

	"github.com/yunussandikci/dbqueue-go/dbqueue"
)

func main() {
	queue, queueErr := dbqueue.NewPostgreSQL("host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable")
	// queue, queueErr := dbqueue.NewMySQL("root:root@tcp(127.0.0.1:3306)/mydb")
	// queue, queueErr := dbqueue.NewSQLite("my.db")
	if queueErr != nil {
		panic(queueErr)
	}

	if createQueueErr := queue.CreateQueue("jobs"); createQueueErr != nil {
		panic(createQueueErr)
	}
	for i := 1; i < 100; i++ {
		err := queue.SendMessage("jobs", &dbqueue.Message{
			DeduplicationID: "asd",
			Payload:         []byte(fmt.Sprintf("message-1")),
		})
		if err != nil {
			panic(err)
		}
	}

	Producer(queue, "jobs")
	go Consumer(queue, "A", "jobs")
	go Consumer(queue, "B", "jobs")

	select {}
}

func Producer(queue *dbqueue.DBQueue, queueName string) {
	for i := 1; i < 1000; i++ {
		if putErr := queue.SendMessage(queueName, &dbqueue.Message{
			Payload: []byte(fmt.Sprintf("message-%d", i)),
		}); putErr != nil {
			panic(putErr)
		}
	}
}

func Consumer(queue *dbqueue.DBQueue, consumerName, queueName string) {
	options := dbqueue.ReceiveMessageOptions{
		VisibilityTimeout:   time.Second * 10,
		MaxNumberOfMessages: 1,
		WaitTime:            0,
	}

	if receiveErr := queue.ReceiveMessage(queueName, func(message dbqueue.Message) {
		fmt.Printf("Consumer:%s %+v\n", consumerName, message)

		// Delete Randomly to test Visibility Timeout
		//if rand.Intn(100) > 20 {
		if deleteErr := queue.DeleteMessage(queueName, message.ID); deleteErr != nil {
			panic(deleteErr)
		}
		//}
	}, options); receiveErr != nil {
		panic(receiveErr)
	}
}
