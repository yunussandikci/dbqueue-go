package main

import (
	"context"
	"fmt"
	"github.com/yunussandikci/dbqueue-go/dbqueue"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
)

func main() {
	//db := "postgres://postgres:root@localhost/example"
	db := "example.db"

	ctx := context.Background()
	engine, connectErr := dbqueue.OpenSQLite(ctx, db)
	if connectErr != nil {
		panic(connectErr)
	}

	queue1, createErr := engine.CreateQueue(ctx, "example")
	if createErr != nil {
		panic(createErr)
	}

	go func() {
		for i := 0; i < 100000; i++ {
			sendErr := queue1.SendMessage(ctx, &types.Message{
				Payload: []byte(fmt.Sprintf("Hello, %d", i)),
			})
			if sendErr != nil {
				panic(sendErr)
			}
		}
	}()

	go func() {
		receiverErr := queue1.ReceiveMessage(ctx, func(message types.ReceivedMessage) {
			fmt.Println(string(message.Payload))
			deleteErr := queue1.DeleteMessage(ctx, message.ID)
			if deleteErr != nil {
				panic(deleteErr)
			}
		}, types.ReceiveMessageOptions{})
		if receiverErr != nil {
			panic(receiverErr)
		}
	}()

	select {}
}
