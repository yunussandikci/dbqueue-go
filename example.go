package main

import (
	"fmt"
	"github.com/yunussandikci/squeuelite-go/squeuelite"
	"strconv"
	"time"
)

func main() {
	squeue, queueErr := squeuelite.New(squeuelite.Config{
		Database:        "my-db",
		Queue:           "default",
		RequeueDuration: time.Minute,
	})
	if queueErr != nil {
		panic(queueErr)
	}

	//Subscribe for Queue
	go func() {
		for item := range squeue.Subscribe(time.Second) {
			fmt.Printf("%s\n", string(item.Payload))
			if doneErr := squeue.Done(item); doneErr != nil {
				return
			}
		}
	}()

	//Subscribe for Queue
	go func() {
		for item := range squeue.Subscribe(time.Second) {
			fmt.Printf("%s\n", string(item.Payload))
			if doneErr := squeue.Done(item); doneErr != nil {
				return
			}
		}
	}()

	//Put Items into Queue
	for i := 1; i < 100; i++ {
		if putErr := squeue.Put(&squeuelite.Message{
			Id:             strconv.Itoa(i),
			Payload:        []byte(fmt.Sprintf("my-message-%d", i)),
			AvailableAfter: time.Now().Add(2 * time.Second),
			Priority:       10,
		}); putErr != nil {
			panic(putErr)
		}
	}

	time.Sleep(1 * time.Minute)
}
