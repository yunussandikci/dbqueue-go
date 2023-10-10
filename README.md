# dbqueue-go

dbqueue-go is a simple, lightweight queue system based on database, implemented in Go. 

## 🚀 Getting Started

To start using dbqueue-go, simply run the following command in your terminal:

```bash
go get github.com/yunussandikci/dbqueue-go
```

Then, import the `dbqueue` package in your Go file:

```go
import "github.com/yunussandikci/dbqueue-go/dbqueue"
```

## 🎉 Usage

### Creating an Instance 
```go
sqliteQueue, err := dbqueue.NewSQLite("foo.db")
mysqlQueue, err := dbqueue.NewMySQL("foo:bar@tcp(127.0.0.1:3306)/baz")
postgresqlQueue, err := dbqueue.NewPostgreSQL("host=localhost user=foo password=bar dbname=baz port=5432 sslmode=disable")
```

### Creating the Queue 

```go
err := dbqueue.CreateQueue("my_queue")
```
The `CreateQueue` function takes one argument:
- `name`: The name of the queue to be created.

### Sending Messages

```go
message := &dbqueue.Message{
    Payload: []byte("Hello, world!"),
}

err := dbqueue.SendMessage("my_queue", message)
```
The `SendMessage` function takes two arguments:
- `queue`: The name of the queue to which the message will be sent.
- `message`: The message to be sent. It's an instance of the `Message` struct.

### Receiving Messages

```go
options := &dbqueue.ReceiveMessageOptions{
    MaxNumberOfMessages: 10,
    VisibilityTimeout:   time.Minute * 10,
    WaitTime:            time.Second * 5,
}

err := dbqueue.ReceiveMessage("my_queue", func(message dbqueue.Message) {
    fmt.Println(string(message.Payload))
}, options)
```
The `ReceiveMessage` function takes three arguments:
- `queue`: The name of the queue from which the messages will be received.
- `f`: A function that will be called for each received message. It takes a `Message` as an argument.
- `options`: An instance of the `ReceiveMessageOptions` struct that specifies options for receiving messages.

### Deleting Messages

```go
err := dbqueue.DeleteMessage("my_queue", "message_id")
```
The `DeleteMessage` function takes two arguments:
- `queue`: The name of the queue from which the message will be deleted.
- `messageId`: The ID of the message to be deleted.

### Purging a Queue

```go
err := dbqueue.PurgeQueue("my_queue")
```
The `PurgeQueue` function takes one argument:
- `queue`: The name of the queue to be purged.

### Changing Message Visibility

```go
err := dbqueue.ChangeMessageVisibility("my_queue", time.Minute*5, "message_id")
```
The `ChangeMessageVisibility` function takes three arguments:
- `queue`: The name of the queue that contains the message.
- `visibilityTimeout`: The new visibility timeout for the message.
- `messageId`: The ID of the message whose visibility will be changed. 

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
