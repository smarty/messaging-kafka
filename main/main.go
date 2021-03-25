package main

import (
	"context"
	"log"
	"time"

	kafka "github.com/smartystreets/messaging-kafka"
	"github.com/smartystreets/messaging/v3"
)

func main() {
	connector := kafka.NewConnector([]string{"localhost:9092"})
	defer func() { _ = connector.Close() }()

	connection, err := connector.Connect(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() { _ = connection.Close() }()

	go func() {
		writer, err1 := connection.CommitWriter(context.Background())
		if err1 != nil {
			panic(err)
		}

		defer func() { _ = writer.Close() }()

		_, _ = writer.Write(context.Background(), messaging.Dispatch{
			SourceID:        1,
			MessageID:       2,
			CorrelationID:   3,
			Timestamp:       time.Now().UTC(),
			Expiration:      0,
			Durable:         false,
			Topic:           "my-topic",
			Partition:       0,
			MessageType:     "sample-message-type",
			ContentType:     "application/json",
			ContentEncoding: "utf8",
			Payload:         []byte("Hello, World!"),
			Headers:         nil,
			Message:         nil,
		})

		err1 = writer.Commit()
		if err1 != nil {
			panic(err1)
		}
	}()

	reader, err := connection.Reader(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() { _ = reader.Close() }()

	stream, err := reader.Stream(context.Background(), messaging.StreamConfig{
		StreamName: "my-topic",
		Partition:  0,
		Sequence:   0,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = stream.Close() }()

	log.Println("[INFO] Waiting for messages on topic...")

	var delivery messaging.Delivery
	for {
		err := stream.Read(context.Background(), &delivery)
		if err != nil {
			panic(err)
		}
		log.Println("Message:", string(delivery.Payload))
	}
}
