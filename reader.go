package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultReader struct {
}

func (this defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	return nil, nil
}

func (d defaultReader) Close() error {
	return nil
}
