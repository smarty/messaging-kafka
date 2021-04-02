package kafka

import (
	"context"
	"strconv"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
	config        configuration
	reader        *kafka.Reader
	consumerGroup bool
	lifecycle     context.Context
	cancel        func()
}

func newStream(config configuration, reader *kafka.Reader, consumerGroup bool, parent context.Context) messaging.Stream {
	this := defaultStream{config: config, reader: reader, consumerGroup: consumerGroup}
	this.lifecycle, this.cancel = context.WithCancel(parent)
	go func() {
		<-this.lifecycle.Done()
		_ = reader.Close()
	}()
	return this
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	raw, err := this.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	target.Upstream = raw
	target.DeliveryID = uint64(raw.Offset)
	//SourceID        uint64
	//MessageID       uint64
	//CorrelationID   uint64 // FUTURE: CausationID and UserID
	target.Timestamp = raw.Time
	target.Durable = true
	//MessageType     string
	//ContentType     string
	//ContentEncoding string
	target.Payload = raw.Value

	for _, header := range raw.Headers {
		switch header.Key {
		case "source-id":
			target.SourceID, _ = strconv.ParseUint(string(header.Value), 10, 64) // TODO: don't do this
		case "message-id":
			target.MessageID, _ = strconv.ParseUint(string(header.Value), 10, 64) // TODO: don't do this
		case "message-type":
			target.MessageType = string(header.Value) // TODO: don't do this
		case "content-type":
			target.ContentType = string(header.Value) // TODO: don't do this
		case "content-encoding":
			target.ContentEncoding = string(header.Value) // TODO: don't do this
		}
	}

	return nil
}

func (this defaultStream) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	if !this.consumerGroup {
		return nil
	}

	messages := make([]kafka.Message, 0, len(deliveries))
	for _, delivery := range deliveries {
		messages = append(messages, delivery.Upstream.(kafka.Message))
	}

	return this.reader.CommitMessages(ctx, messages...)
}

func (this defaultStream) Close() error {
	this.cancel()
	return nil
}
