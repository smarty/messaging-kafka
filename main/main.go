package main

import (
	"context"
	"log"

	kafka "github.com/smarty/messaging-kafka"
	"github.com/smarty/messaging/v4"
	"github.com/smarty/messaging/v4/handlers/retry"
	"github.com/smarty/messaging/v4/streaming"
)

type myMessageHandler struct{}

func (this myMessageHandler) Handle(_ context.Context, messages ...interface{}) {
	for _, message := range messages {
		delivery := message.(messaging.Delivery)
		log.Println("DeliveryID", delivery.DeliveryID)
		log.Println("Topic", delivery.Topic)
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
			streaming.SubscriptionOptions.AddStream(
				streaming.StreamOptions.StreamName("my-topic-a"),
				streaming.StreamOptions.Sequence(2293),
			),
			streaming.SubscriptionOptions.AddStream(
				streaming.StreamOptions.StreamName("my-topic-b"),
				streaming.StreamOptions.Sequence(2292),
			),
			streaming.SubscriptionOptions.AddStream(
				streaming.StreamOptions.StreamName("my-topic-c"),
				streaming.StreamOptions.Sequence(2286),
			),
			streaming.SubscriptionOptions.FullDeliveryToHandler(true),
			streaming.SubscriptionOptions.AddWorkers(retry.New(myMessageHandler{})),
		),
	)

	defer func() { _ = connector.Close() }()

	connection, err := connector.Connect(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() { _ = connection.Close() }()

	//go func() {
	//	time.Sleep(time.Second)
	//	writer, err1 := connection.CommitWriter(context.Background())
	//	if err1 != nil {
	//		panic(err)
	//	}
	//
	//	defer func() { _ = writer.Close() }()
	//
	//	i := 0
	//	for {
	//		topicSuffix := "-a"
	//		if modulo := i % 3; modulo == 1 {
	//			topicSuffix = "-b"
	//		} else if modulo == 2 {
	//			topicSuffix = "-c"
	//		}
	//
	//		topic := "my-topic" + topicSuffix
	//		// log.Println("[INFO] Writing message to:", topic)
	//
	//		_, err = writer.Write(context.Background(), messaging.Dispatch{
	//			SourceID:      1,
	//			MessageID:     2,
	//			CorrelationID: 3,
	//			Timestamp:     time.Now().UTC(),
	//			Expiration:    0,
	//			Durable:       false,
	//			Topic:         topic,
	//			Partition:     0,
	//			MessageType:   "sample-message-type",
	//			ContentType:   "application/json",
	//			Payload:       []byte("Hello, World!"),
	//			Headers:       nil,
	//			Message:       nil,
	//		})
	//
	//		if err != nil {
	//			panic(err)
	//		}
	//
	//		err1 = writer.Commit()
	//		if err1 != nil {
	//			panic(err1)
	//		}
	//		i++
	//		//log.Println("[INFO] Message written.", i)
	//		// time.Sleep(time.Second / 4)
	//	}
	//}()

	consumer.Listen()
}
