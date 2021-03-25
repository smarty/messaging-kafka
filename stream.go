package kafka

import (
	"context"
	"io"

	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
}

func newStream() messaging.Stream {
	return nil
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	return nil
	//select {
	//case source, open := <-this.stream.Messages():
	//	return this.processDelivery(source, target, open)
	//case <-ctx.Done():
	//	return ctx.Err()
	//}
}
func (this defaultStream) processDelivery(target *messaging.Delivery, deliveryChannelOpen bool) error {
	if !deliveryChannelOpen {
		return io.EOF
	}

	return nil
}

func (this defaultStream) Acknowledge(context.Context, ...messaging.Delivery) error {
	return nil
}

func (this defaultStream) Close() error {
	return nil
}
