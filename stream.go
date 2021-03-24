package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
}

func (this defaultStream) Read(ctx context.Context, delivery *messaging.Delivery) error {
	return nil
}

func (this defaultStream) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	return nil
}

func (this defaultStream) Close() error {
	return nil
}
