package kafka

import (
	"context"
	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
}

func (this defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	return nil, nil
}

func (this defaultConnector) Close() error {
	return nil
}
