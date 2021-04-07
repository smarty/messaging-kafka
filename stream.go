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
	go this.awaitCancel()
	return this
}
func (this defaultStream) awaitCancel() {
	<-this.lifecycle.Done()
	_ = this.reader.Close()
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	raw, err := this.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	target.Upstream = raw
	target.DeliveryID = uint64(raw.Offset)
	target.Timestamp = raw.Time
	target.Durable = true
	target.Payload = raw.Value

	for _, header := range raw.Headers {
		switch header.Key {
		case "source-id":
			target.SourceID, _ = strconv.ParseUint(string(header.Value), 10, 64)
		case "message-id":
			target.MessageID, _ = strconv.ParseUint(string(header.Value), 10, 64)
		case "correlation-id":
			target.CorrelationID, _ = strconv.ParseUint(string(header.Value), 10, 64)
		case "message-type":
			target.MessageType = string(header.Value)
		case "content-type":
			target.ContentType = string(header.Value)
		case "content-encoding":
			target.ContentEncoding = string(header.Value)
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
