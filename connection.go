package kafka

import (
	"context"
	"io"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnection struct {
	config configuration
	active []io.Closer
	mutex  *sync.Mutex
}

func newConnection(config configuration) messaging.Connection {
	return &defaultConnection{config: config, mutex: &sync.Mutex{}}
}

func (this *defaultConnection) Reader(_ context.Context) (messaging.Reader, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	reader := newReader(this.config)
	this.active = append(this.active, reader)
	return reader, nil
}

func (this *defaultConnection) Writer(_ context.Context) (messaging.Writer, error) {
	return nil, nil
}

func (this *defaultConnection) CommitWriter(_ context.Context) (messaging.CommitWriter, error) {
	return nil, nil
}

func (this *defaultConnection) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i := range this.active {
		_ = this.active[i].Close()
		this.active[i] = nil
	}
	this.active = this.active[0:0]

	return nil
}
