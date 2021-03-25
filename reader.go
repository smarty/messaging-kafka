package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultReader struct {
}

func newReader() messaging.Reader {
	return nil
}

func (this defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	return nil, nil
}

func (this defaultReader) Close() error {
	return nil
}
