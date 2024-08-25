
# DBQueue-Go

DBQueue-Go is a simple, lightweight queue system based on various SQL databases, implemented in Go. It supports MySQL, PostgreSQL, and SQLite, allowing for easy integration with existing database infrastructures.

## 🚀 Getting Started

To start using DBQueue-Go, simply run the following command in your terminal:

```bash
go get github.com/yunussandikci/dbqueue-go
```

Then, import the `dbqueue` package in your Go file:

```go
import "github.com/yunussandikci/dbqueue-go/dbqueue"
```

## 🎉 Usage

### Opening a Database Connection

To work with DBQueue-Go, you need to open a connection to your preferred SQL database engine:

```go
postgresqlEngine, _ := dbqueue.OpenPostgreSQL(ctx, "host=localhost user=foo password=bar dbname=baz port=5432 sslmode=disable")
mysqlEngine, _ := dbqueue.OpenMySQL(ctx, "foo:bar@tcp(127.0.0.1:3306)/baz")
sqliteEngine, _ := dbqueue.OpenSQLite(ctx, "foo.db")
```

### Creating a Queue

Create a new queue using the engine:

```go
queue, _ := postgresqlEngine.CreateQueue(ctx, "my_queue")
```

### Sending Messages

To send a message to the queue:

```go
_ = queue.SendMessage(ctx, &types.Message{
    Payload:  []byte("Hello, world!"),
    Priority: 1,
})
```

### Sending Messages in Batch

You can also send multiple messages at once:

```go
_ = queue.SendMessageBatch(ctx, []*types.Message{
    {Payload: []byte("Message 1"), Priority: 1},
    {Payload: []byte("Message 2"), Priority: 1},
})
```

### Receiving Messages

Receive messages from the queue:

```go
_ = queue.ReceiveMessage(ctx, func(message types.ReceivedMessage) {
    fmt.Println("Received message:", string(message.Payload))
}, &types.ReceiveMessageOptions{
    MaxNumberOfMessages: common.Ptr(10),
    VisibilityTimeout:   common.Ptr(30 * time.Second),
    WaitTime:            common.Ptr(5 * time.Second),
})
```

### Deleting Messages

Delete a specific message from the queue:

```go
_ = queue.DeleteMessage(ctx, messageID)
```

You can also delete multiple messages at once:

```go
_ = queue.DeleteMessageBatch(ctx, []uint{messageID1, messageID2})
```

### Changing Message Visibility

Change the visibility timeout of a message:

```go
_ = queue.ChangeMessageVisibility(ctx, messageID, time.Minute*5)
```

This can also be done in batch:

```go
_ = queue.ChangeMessageVisibilityBatch(ctx, []uint{messageID1, messageID2}, time.Minute*5)
```

### Deleting a Queue

Delete a queue if it is no longer needed:

```go
_ = engineInstance.DeleteQueue(ctx, "my_queue")
```

### Purging a Queue

Purge all messages from a queue:

```go
_ = engineInstance.PurgeQueue(ctx, "my_queue")
```

## 🤝 Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

### Issues

If you encounter any bugs or have suggestions for improvements, please feel free to [open an issue](https://github.com/yunussandikci/dbqueue-go/issues). Your feedback is invaluable and helps us improve the project.

### Pull Requests

We welcome pull requests for new features, bug fixes, or documentation improvements. Here’s how you can contribute:

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.