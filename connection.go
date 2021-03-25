package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnection struct {
}

func newConnection() messaging.Connection {
	return defaultConnection{}
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
