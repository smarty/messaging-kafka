package kafka

import (
	"context"
	"sync"

	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	active []messaging.Connection
	mutex  *sync.Mutex
}

func NewConnector(brokers []string) messaging.Connector {
	return &defaultConnector{mutex: &sync.Mutex{}}
}

func (this *defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return nil, nil

	//this.active = append(this.active, newConnection(client))
	//return this.active[len(this.active)-1], nil
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
