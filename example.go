package main

import (
	"fmt"
	"github.com/yunussandikci/squeuelite-go/squeuelite"
	"math/rand"
	"time"
)

func main() {
	squeue, queueErr := squeuelite.New("my-db")
	if queueErr != nil {
		panic(queueErr)
	}

	//Create Queue
	if createQueueErr := squeue.CreateQueue("messages"); createQueueErr != nil {
		panic(createQueueErr)
	}

	//Send items into Queue
	for i := 1; i < 100; i++ {
		if putErr := squeue.SendMessage("messages", &squeuelite.Message{
			Payload:  []byte(fmt.Sprintf("message-%d", i)),
			Priority: rand.Uint32() % 100,
		}); putErr != nil {
			panic(putErr)
		}
	}

	//Subscribe for Queue
	if receiverErr := squeue.ReceiveMessage("messages", func(message squeuelite.Message) {
		fmt.Printf("Payload:%s Priority:%d Retry:%d\n", string(message.Payload), message.Priority, message.Retry)

		// Delete Messages Randomly
		if rand.Intn(100) > 40 {
			if deleteErr := squeue.DeleteMessage("messages", message.Id); deleteErr != nil {
				panic(deleteErr)
			}
		}
	}, squeuelite.ReceiveMessageOptions{
		VisibilityTimeout: 5 * time.Second,
		WaitTime:          time.Second,
	}); receiverErr != nil {
		panic(receiverErr)
	}

}
