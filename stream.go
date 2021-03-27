package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
	config configuration
	reader *kafka.Reader
}

func newStream(config configuration, reader *kafka.Reader) messaging.Stream {
	return defaultStream{config: config, reader: reader}
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	raw, err := this.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

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
	// NOTE: this only makes sense when we're using consumer groups

	last := messaging.Delivery{} // TODO: get last delivery
	return this.reader.CommitMessages(ctx, kafka.Message{
		Topic:     "", // TODO
		Partition: 0,  // TODO
		Offset:    int64(last.DeliveryID + 1),
	})
}

func (this defaultStream) Close() error {
	return this.reader.Close()
}
