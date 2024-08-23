package main

import (
	"context"
	"fmt"
	"github.com/yunussandikci/dbqueue-go/dbqueue"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"time"
)

type MyMessage struct {
	UserId int
}

func main() {
	ctx := context.Background()
	engine, err := dbqueue.Connect(ctx, "postgres://postgres:root@localhost/example")
	if err != nil {
		panic(err)
	}

	queue1, createErr := engine.CreateQueue("example")
	if err != nil {
		panic(createErr)
	}

	// put 100 message to queue
	for i := 0; i < 10; i++ {
		message := types.Message{
			Payload: []byte(fmt.Sprintf("Hello %d", i)),
		}
		sendErr := queue1.SendMessage(&message)
		if sendErr != nil {
			panic(sendErr)
		}
	}

	receiverErr := queue1.ReceiveMessage(func(message types.ReceivedMessage) {
		fmt.Println(string(message.Payload))
		time.Sleep(1 * time.Second)
	}, types.ReceiveMessageOptions{
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   10 * time.Second,
		WaitTime:            0,
	})
	if receiverErr != nil {
		panic(receiverErr)
	}

}
