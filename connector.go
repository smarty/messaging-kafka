package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	broker string
	active []messaging.Connection
	mutex  *sync.Mutex
}

func NewConnector(broker string) messaging.Connector {
	return &defaultConnector{broker: broker, mutex: &sync.Mutex{}}
}

func (this *defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	conn, err := kafka.DialContext(ctx, "tcp", this.broker)
	if err != nil {
		return nil, err
	}

	this.active = append(this.active, newConnection(conn))
	return this.active[len(this.active)-1], nil
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
