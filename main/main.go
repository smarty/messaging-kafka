package main

import (
	"context"
	"log"
	"time"

	kafka "github.com/smartystreets/messaging-kafka"
	"github.com/smartystreets/messaging/v3"
)

func main() {
	connector := kafka.NewConnector()
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

		i := 0
		for {
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
			i++
			log.Println("[INFO] Message written.", i)
		}
	}()

	reader, err := connection.Reader(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() { _ = reader.Close() }()

	stream, err := reader.Stream(context.Background(), messaging.StreamConfig{
		GroupName: "my-group",
		Topics:    []string{"my-topic"},
		//StreamName: "my-topic",
		//Partition:  0,
		//Sequence:   0,
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

		log.Println("DeliveryID", delivery.DeliveryID)
		//log.Println("SourceID", delivery.SourceID)
		//log.Println("MessageID", delivery.MessageID)
		//log.Println("CorrelationID", delivery.CorrelationID)
		//log.Println("Timestamp", delivery.Timestamp)
		//log.Println("Durable", delivery.Durable)
		//log.Println("MessageType", delivery.MessageType)
		//log.Println("ContentType", delivery.ContentType)
		//log.Println("ContentEncoding", delivery.ContentEncoding)
		//log.Println("Payload", string(delivery.Payload))
		//log.Println("------------------------------")

		err = stream.Acknowledge(context.Background(), delivery)
		if err != nil {
			panic(err)
		}
	}
}
