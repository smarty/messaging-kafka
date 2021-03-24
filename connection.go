package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/smartystreets/messaging/v3"
)

type defaultConnection struct {
	client sarama.Client
}

func newConnection(client sarama.Client) messaging.Connection {
	return defaultConnection{client: client}
}

func (this defaultConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	consumer, err := sarama.NewConsumerFromClient(this.client)
	if err != nil {
		return nil, err
	}

	return newReader(consumer), nil
}

func (this defaultConnection) Writer(ctx context.Context) (messaging.Writer, error) {
	return nil, nil
}

func (this defaultConnection) CommitWriter(ctx context.Context) (messaging.CommitWriter, error) {
	return nil, nil
}

func (this defaultConnection) Close() error {
	return nil
}
