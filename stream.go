package kafka

import (
	"context"

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
	//SourceID        uint64
	//MessageID       uint64
	//CorrelationID   uint64 // FUTURE: CausationID and UserID
	target.Timestamp = raw.Time
	target.Durable = true
	//MessageType     string
	//ContentType     string
	//ContentEncoding string
	target.Payload = raw.Value

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
