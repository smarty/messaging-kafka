package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultWriter struct {
}

func newWriter() messaging.CommitWriter {
	return nil
}

func (this defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	return 0, nil
}

func (this defaultWriter) Commit() error {
	return nil
}
func (this defaultWriter) Rollback() error {
	return nil
}

func (this defaultWriter) Close() error {
	return nil
}
