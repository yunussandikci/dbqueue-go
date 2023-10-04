# SQueueLite-Go

SQueueLite-Go is a simple, lightweight queue system implemented in Go. It uses SQLite as its database backend.

## 🚀 Getting Started

To start using SQueueLite-Go, simply run the following command in your terminal:

```bash
go get github.com/yunussandikci/squeuelite-go
```

Then, import the `squeuelite` package in your Go file:

```go
import "github.com/yunussandikci/squeuelite-go/squeuelite"
```

## 🎉 Usage

### Creating a Queue

```go
err := squeuelite.CreateQueue("my_queue")
```
The `CreateQueue` function takes one argument:
- `name`: The name of the queue to be created.

### Sending Messages

```go
message := &squeuelite.Message{
    Payload: []byte("Hello, world!"),
}

err := squeuelite.SendMessage("my_queue", message)
```
The `SendMessage` function takes two arguments:
- `queue`: The name of the queue to which the message will be sent.
- `message`: The message to be sent. It's an instance of the `Message` struct.

### Receiving Messages

```go
options := &squeuelite.ReceiveMessageOptions{
    MaxNumberOfMessages: 10,
    VisibilityTimeout:   time.Minute * 10,
    WaitTime:            time.Second * 5,
}

err := squeuelite.ReceiveMessage("my_queue", func(message squeuelite.Message) {
    fmt.Println(string(message.Payload))
}, options)
```
The `ReceiveMessage` function takes three arguments:
- `queue`: The name of the queue from which the messages will be received.
- `f`: A function that will be called for each received message. It takes a `Message` as an argument.
- `options`: An instance of the `ReceiveMessageOptions` struct that specifies options for receiving messages.

### Deleting Messages

```go
err := squeuelite.DeleteMessage("my_queue", "message_id")
```
The `DeleteMessage` function takes two arguments:
- `queue`: The name of the queue from which the message will be deleted.
- `messageId`: The ID of the message to be deleted.

### Purging a Queue

```go
err := squeuelite.PurgeQueue("my_queue")
```
The `PurgeQueue` function takes one argument:
- `queue`: The name of the queue to be purged.

### Changing Message Visibility

```go
err := squeuelite.ChangeMessageVisibility("my_queue", time.Minute*5, "message_id")
```
The `ChangeMessageVisibility` function takes three arguments:
- `queue`: The name of the queue that contains the message.
- `visibilityTimeout`: The new visibility timeout for the message.
- `messageId`: The ID of the message whose visibility will be changed. 

## 📜 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
