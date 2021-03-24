package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultWriter struct {
}

func (this defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	return 0, nil
}

func (this defaultWriter) Close() error {
	return nil
}
