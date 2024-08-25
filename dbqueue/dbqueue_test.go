package dbqueue

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
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

func Test_ReadWriteDelete_PostgreSQL_5Receiver_5Sender(t *testing.T) {
	// given
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenPostgreSQL(ctx, postgres.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testReadWriteDelete(t, engine, 5, 5, 5000)
}
func Test_Retrieval_PostgreSQL(t *testing.T) {
	// given
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenPostgreSQL(ctx, postgres.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testRetrieval(t, engine)
}
func Test_Priority_PostgreSQL(t *testing.T) {
	// given
	ctx := context.Background()
	postgres, runErr := postgres.Run(ctx, "docker.io/postgres:16", postgres.WithDatabase("test"),
		postgres.WithUsername("test"), postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenPostgreSQL(ctx, postgres.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testPriority(t, engine)
}
func Test_ReadWriteDelete_MySQL_5Receiver_5Sender(t *testing.T) {
	// given
	ctx := context.Background()
	mysql, runErr := mysql.Run(ctx, "mysql:8", mysql.WithDatabase("test"),
		mysql.WithUsername("test"), mysql.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  MySQL Community Server - GPL").
			WithStartupTimeout(10*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenMySQL(ctx, mysql.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testReadWriteDelete(t, engine, 5, 5, 5000)
}
func Test_Retrieval_MySQL(t *testing.T) {
	// given
	ctx := context.Background()
	mysql, runErr := mysql.Run(ctx, "mysql:8", mysql.WithDatabase("test"),
		mysql.WithUsername("test"), mysql.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  MySQL Community Server - GPL").
			WithStartupTimeout(10*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenMySQL(ctx, mysql.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testRetrieval(t, engine)
}
func Test_Priority_MySQL(t *testing.T) {
	// given
	ctx := context.Background()
	mysql, runErr := mysql.Run(ctx, "mysql:8", mysql.WithDatabase("test"),
		mysql.WithUsername("test"), mysql.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  MySQL Community Server - GPL").
			WithStartupTimeout(10*time.Second)),
	)
	if runErr != nil {
		t.Fatal(runErr)
	}

	engine, openErr := OpenMySQL(ctx, mysql.MustConnectionString(ctx))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testRetrieval(t, engine)
}
func Test_ReadWriteDelete_SQLite_5Receiver_5Sender(t *testing.T) {
	// given
	ctx := context.Background()
	db, dbErr := os.CreateTemp("", "")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	engine, openErr := OpenSQLite(ctx, fmt.Sprintf("file:%s?_journal_mode=WAL", db.Name()))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testReadWriteDelete(t, engine, 5, 5, 5000)
}
func Test_Retrieval_SQLite(t *testing.T) {
	// given
	ctx := context.Background()
	db, dbErr := os.CreateTemp("", "")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	engine, openErr := OpenSQLite(ctx, fmt.Sprintf("file:%s?_journal_mode=WAL", db.Name()))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testRetrieval(t, engine)
}
func Test_Priority_SQLite(t *testing.T) {
	// given
	ctx := context.Background()
	db, dbErr := os.CreateTemp("", "")
	if dbErr != nil {
		t.Fatal(dbErr)
	}

	engine, openErr := OpenSQLite(ctx, fmt.Sprintf("file:%s?_journal_mode=WAL", db.Name()))
	if openErr != nil {
		t.Fatal(openErr)
	}

	// when & then
	testPriority(t, engine)
}
func testRetrieval(t *testing.T, engine types.Engine) {
	// given
	ctx := context.Background()
	queue, createErr := engine.CreateQueue(ctx, "test")
	if createErr != nil {
		t.Fatal(createErr)
	}
	sendErr := queue.SendMessage(ctx, &types.Message{
		Payload: []byte("1"),
	})
	assert.NoError(t, sendErr)

	// when
	finished := make(chan bool)
	var messages []types.ReceivedMessage
	go func() {
		receiveErr := queue.ReceiveMessage(ctx, func(message types.ReceivedMessage) {
			messages = append(messages, message)
			if len(messages) == 10 {
				close(finished)
			}
		}, types.ReceiveMessageOptions{
			VisibilityTimeout: common.Ptr(50 * time.Millisecond),
			WaitTime:          common.Ptr(100 * time.Millisecond),
		})
		assert.NoError(t, receiveErr)
	}()
	<-finished

	// then
	assert.Len(t, messages, 10)
	for i, message := range messages {
		assert.Equal(t, uint32(i+1), message.Retrieval)
	}
}
func testPriority(t *testing.T, engine types.Engine) {
	// given
	ctx := context.Background()
	queue, createErr := engine.CreateQueue(ctx, "test")
	if createErr != nil {
		t.Fatal(createErr)
	}
	for i := 1; i <= 10; i++ {
		sendErr := queue.SendMessage(ctx, &types.Message{
			Payload:  []byte(strconv.Itoa(10 - i)),
			Priority: uint32(10 - i),
		})
		assert.NoError(t, sendErr)
	}

	// when
	finished := make(chan bool)
	var messages []types.ReceivedMessage
	go func() {
		receiveErr := queue.ReceiveMessage(ctx, func(message types.ReceivedMessage) {
			messages = append(messages, message)
			if len(messages) == 10 {
				close(finished)
			}
		}, types.ReceiveMessageOptions{
			VisibilityTimeout: common.Ptr(50 * time.Millisecond),
			WaitTime:          common.Ptr(100 * time.Millisecond),
		})
		assert.NoError(t, receiveErr)
	}()
	<-finished

	// then
	assert.Len(t, messages, 10)
	last := int(^uint(0) >> 1)
	for _, message := range messages {
		payload, _ := strconv.Atoi(string(message.Payload))
		assert.Greater(t, last, payload)
		last = payload
	}
}
func testReadWriteDelete(t *testing.T, engine types.Engine, receiverCount, senderCount, limit int) {
	ctx := context.Background()
	queue, createErr := engine.CreateQueue(ctx, "test")
	if createErr != nil {
		t.Fatal(createErr)
	}

	sender := func(num int) {
		for i := 1; i <= limit/senderCount; i++ {
			sendErr := queue.SendMessage(ctx, &types.Message{
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
		_ = queue.ReceiveMessage(ctx, func(message types.ReceivedMessage) {
			if deleteErr := queue.DeleteMessage(ctx, message.ID); deleteErr != nil {
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

	assert.NoError(t, engine.DeleteQueue(ctx, "test"))
	fmt.Printf("%d messages processed\n", limit)
}
