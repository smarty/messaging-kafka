package kafka

import (
	"context"
	"io"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	config configuration
	active []io.Closer
	mutex  *sync.Mutex
}

func NewConnector(options ...option) messaging.Connector {
	config := configuration{}
	Options.apply(options...)(&config)
	return &defaultConnector{config: config, mutex: &sync.Mutex{}}
}

func (this *defaultConnector) Connect(_ context.Context) (messaging.Connection, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	connection := newConnection(this.config)
	this.active = append(this.active, connection)
	return connection, nil
}

func (this *defaultConnector) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i := range this.active {
		_ = this.active[i].Close()
		this.active[i] = nil
	}
	this.active = this.active[0:0]

	return nil
}
