package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v4"
)

type defaultConnection struct {
	config    configuration
	lifecycle context.Context
	cancel    context.CancelFunc
}

func newConnection(config configuration, parent context.Context) messaging.Connection {
	this := &defaultConnection{config: config} // pointer because defaultConnection needs to be comparable for connection pool
	this.lifecycle, this.cancel = context.WithCancel(parent)
	return this
}

func (this *defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	return newReader(this.config, this.lifecycle), nil
}

func (this *defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	return newWriter(this.config, this.lifecycle, false), nil
}

func (this *defaultConnection) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	return newWriter(this.config, this.lifecycle, true), nil
}

func (this *defaultConnection) Close() error {
	this.cancel()
	return nil
}
