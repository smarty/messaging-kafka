package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultConnection struct {
	inner *kafka.Conn
}

func newConnection(inner *kafka.Conn) messaging.Connection {
	return defaultConnection{inner: inner}
}

func (this defaultConnection) Reader(ctx context.Context) (messaging.Reader, error) {
	return nil, nil
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
