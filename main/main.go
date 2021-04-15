package main

import (
	"context"
	"log"
	"time"

	"github.com/smartystreets/messaging-kafka"
	"github.com/smartystreets/messaging/v3"
	"github.com/smartystreets/messaging/v3/handlers/retry"
	"github.com/smartystreets/messaging/v3/streaming"
)

type myMessageHandler struct{}

func (this myMessageHandler) Handle(_ context.Context, messages ...interface{}) {
	for _, message := range messages {
		delivery := message.(messaging.Delivery)
		log.Println("DeliveryID", delivery.DeliveryID)
		log.Println("MessageType", delivery.MessageType)
		log.Println("ContentType", delivery.ContentType)
		log.Println("Payload", string(delivery.Payload))
		log.Println("-----------------------")
	}
}

func main() {
	logger := log.Default()
	connector := kafka.NewConnector(
		kafka.Options.MessageTypeIdentifiers(map[uint32]string{
			42: "sample-message-type",
		}),
		kafka.Options.ContentTypeIdentifiers(map[uint8]string{
			3: "application/json",
		}),
		kafka.Options.Logger(logger),
	)

	consumer := streaming.New(connector,
		streaming.Options.Logger(logger),
		streaming.Options.Subscriptions(
			streaming.NewSubscription("my-topic",
				// streaming.SubscriptionOptions.Name("my-group"),
				streaming.SubscriptionOptions.FullDeliveryToHandler(true),
				streaming.SubscriptionOptions.AddWorkers(
					retry.New(myMessageHandler{}),
				),
			),
		),
	)

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
			_, err = writer.Write(context.Background(), messaging.Dispatch{
				SourceID:      1,
				MessageID:     2,
				CorrelationID: 3,
				Timestamp:     time.Now().UTC(),
				Expiration:    0,
				Durable:       false,
				Topic:         "my-topic",
				Partition:     0,
				MessageType:   "sample-message-type",
				ContentType:   "application/json",
				Payload:       []byte("Hello, World!"),
				Headers:       nil,
				Message:       nil,
			})

			if err != nil {
				panic(err)
			}

			err1 = writer.Commit()
			if err1 != nil {
				panic(err1)
			}
			i++
			//log.Println("[INFO] Message written.", i)
			time.Sleep(time.Second / 4)
		}
	}()

	consumer.Listen()
}
