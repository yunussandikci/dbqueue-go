package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/yunussandikci/dbqueue-go/dbqueue"
)

func main() {
	before := time.Now()
	//queue, queueErr := dbqueue.NewPostgreSQL("host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable")
	//queue, queueErr := dbqueue.NewMySQL("root:root@tcp(127.0.0.1:3306)/mydb")
	queue, queueErr := dbqueue.NewSQLite("my.db")
	if queueErr != nil {
		panic(queueErr)
	}

	if createQueueErr := queue.CreateQueue("jobs"); createQueueErr != nil {
		panic(createQueueErr)
	}

	Producer(queue, "jobs")

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		tmpI := i
		go func() {
			Consumer(queue, fmt.Sprintf("Consumer %d", tmpI), "jobs")
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Println(time.Since(before))
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
		VisibilityTimeout:   time.Minute * 10,
		MaxNumberOfMessages: 1,
		WaitTime:            0,
	}

	if receiveErr := queue.ReceiveMessage(queueName, func(message dbqueue.Message) {
		fmt.Printf("%s %+v\n", consumerName, message)
		if deleteErr := queue.DeleteMessage(queueName, message.ID); deleteErr != nil {
			panic(deleteErr)
		}
	}, options); receiveErr != nil {
		panic(receiveErr)
	}
}
