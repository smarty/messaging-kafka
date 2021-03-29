package kafka

import (
	"context"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	config    configuration
	lifecycle context.Context
	cancel    func()
}

func NewConnector(options ...option) messaging.Connector {
	config := configuration{}
	Options.apply(options...)(&config)
	this := defaultConnector{config: config}
	this.lifecycle, this.cancel = context.WithCancel(config.Context)
	return this
}

func (this defaultConnector) Connect(_ context.Context) (messaging.Connection, error) {
	return newConnection(this.config, this.lifecycle), nil
}

func (this defaultConnector) Close() error {
	this.cancel()
	return nil
}
