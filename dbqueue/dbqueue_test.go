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

func TestPostgreSQL_10k_Sync(t *testing.T) {
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test_db"),
		postgres.WithUsername("test_user"), postgres.WithPassword("test_password"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}
	db := postgres.MustConnectionString(ctx)

	test(t, false, db, 10000, types.ReceiveMessageOptions{
		MaxNumberOfMessages: common.Ptr(1),
	})
}

func TestPostgreSQL_10k_Async(t *testing.T) {
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	test(t, true, postgres.MustConnectionString(ctx), 10000, types.ReceiveMessageOptions{
		MaxNumberOfMessages: common.Ptr(1),
	})
}

func TestSQLite_10k_Sync(t *testing.T) {
	db, dbErr := os.CreateTemp("", "testdb")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	test(t, false, db.Name(), 10000, types.ReceiveMessageOptions{
		MaxNumberOfMessages: common.Ptr(1),
	})
}

func TestSQLite_10k_Async(t *testing.T) {
	db, dbErr := os.CreateTemp("", "testdb")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	test(t, true, db.Name(), 10000, types.ReceiveMessageOptions{
		MaxNumberOfMessages: common.Ptr(1),
	})
}

func test(t *testing.T, async bool, db string, limit int, options types.ReceiveMessageOptions) {
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

	sender := func() {
		for i := 1; i <= limit; i++ {
			sendErr := queue.SendMessage(&types.Message{
				Payload: []byte(strconv.Itoa(i)),
			})
			assert.NoError(t, sendErr)
			if i%1000 == 0 {
				fmt.Printf("Sent %d/%d\n", i, limit)
			}
		}
	}

	receiveCounter := atomic.Uint64{}
	receiveFinished := make(chan bool)
	receiver := func() {
		var ()
		_ = queue.ReceiveMessage(func(message types.ReceivedMessage) {
			if deleteErr := queue.DeleteMessage(message.ID); deleteErr != nil {
				assert.NoError(t, deleteErr)
			}
			receiveCounter.Add(1)
			count := receiveCounter.Load()
			if count%1000 == 0 {
				fmt.Printf("Processed %d/%d\n", count, limit)
			}
			if count == uint64(limit) {
				close(receiveFinished)
			}
		}, options)
	}

	if async {
		go sender()
		go receiver()
	} else {
		sender()
		go receiver()
	}

	<-receiveFinished

	assert.NoError(t, engine.DeleteQueue("test"))
	fmt.Printf("%d messages processed in %s\n", limit, time.Since(now))
}
