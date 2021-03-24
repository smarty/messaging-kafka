package kafka

import (
	"context"
	"io"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
	stream sarama.PartitionConsumer
}

func newStream(stream sarama.PartitionConsumer) messaging.Stream {
	return defaultStream{stream: stream}
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	select {
	case source, open := <-this.stream.Messages():
		return this.processDelivery(source, target, open)
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (this defaultStream) processDelivery(source *sarama.ConsumerMessage, target *messaging.Delivery, deliveryChannelOpen bool) error {
	if !deliveryChannelOpen {
		return io.EOF
	}

	// TODO: we may want the partition through into the target delivery
	target.DeliveryID = uint64(source.Offset)
	target.Timestamp = source.Timestamp
	target.Durable = true
	target.Payload = source.Value

	// This is quick, sample code. We really, REALLY don't want to do all of these string operations. Instead, we'll
	// most likely use numeric values and avoid strings wherever possible
	for _, header := range source.Headers {
		switch string(header.Key) {
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

	//this.monitor.DeliveryReceived()
	return nil
}

func (this defaultStream) Acknowledge(context.Context, ...messaging.Delivery) error {
	return nil
}

func (this defaultStream) Close() error {
	this.stream.AsyncClose()
	return nil
}
