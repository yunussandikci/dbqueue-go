package dbqueue

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/yunussandikci/dbqueue-go/dbqueue/common"
	"github.com/yunussandikci/dbqueue-go/dbqueue/types"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func Test_PostgreSQL_10kMessage_1Receiver_1Sender(t *testing.T) {
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}
	db := postgres.MustConnectionString(ctx)

	testSendReceiveDelete(t, 1, 1, 10000, db)
}

func Test_PostgreSQL_10kMessage_5Receiver_5Sender(t *testing.T) {
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	testSendReceiveDelete(t, 5, 5, 10000, postgres.MustConnectionString(ctx))
}

func Test_SQLite_10kMessage_1Receiver_1Sender(t *testing.T) {
	db, dbErr := os.CreateTemp("", "")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	testSendReceiveDelete(t, 1, 1, 10000, fmt.Sprintf("file:%s?_journal_mode=WAL", db.Name()))
}

func Test_SQLite_10kMessage_5Receiver_5Sender(t *testing.T) {
	db, dbErr := os.CreateTemp("", "")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	testSendReceiveDelete(t, 5, 5, 10000, fmt.Sprintf("file:%s?_journal_mode=WAL", db.Name()))
}

func testSendReceiveDelete(t *testing.T, receiverCount, senderCount, limit int, db string) {
	now := time.Now()
	ctx := context.Background()
	engine, connectErr := Connect(ctx, db)
	if connectErr != nil {
		t.Fatal(connectErr)
	}
	queue, createErr := engine.CreateQueue("test")
	if createErr != nil {
		t.Fatal(createErr)
	}

	sender := func(num int) {
		for i := 1; i <= limit/senderCount; i++ {
			sendErr := queue.SendMessage(&types.Message{
				Payload: []byte(strconv.Itoa(i)),
			})
			assert.NoError(t, sendErr)
			if i%((limit/senderCount)/10) == 0 {
				fmt.Printf("[SENDER %d]: %d/%d\n", num, i, limit/senderCount)
			}
		}
	}

	receiveCounter := atomic.Uint64{}
	receiveFinished := make(chan bool)
	receiver := func(num int) {
		_ = queue.ReceiveMessage(func(message types.ReceivedMessage) {
			if deleteErr := queue.DeleteMessage(message.ID); deleteErr != nil {
				assert.NoError(t, deleteErr)
			}
			receiveCounter.Add(1)
			count := receiveCounter.Load()
			if count%1000 == 0 {
				fmt.Printf("[RECEIVER %d]: %d/%d\n", num, count, limit)
			}
			if count == uint64(limit) {
				close(receiveFinished)
			}
		}, types.ReceiveMessageOptions{
			MaxNumberOfMessages: common.Ptr(1),
		})
	}

	for i := 0; i < receiverCount; i++ {
		go receiver(i)
	}
	for i := 0; i < senderCount; i++ {
		go sender(i)
	}

	<-receiveFinished

	assert.NoError(t, engine.DeleteQueue("test"))
	fmt.Printf("%d messages processed in %s\n", limit, time.Since(now))
}
