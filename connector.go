package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/smartystreets/messaging/v3"
)

type defaultConnector struct {
	config  *sarama.Config
	brokers []string

	active []messaging.Connection
	mutex  *sync.Mutex
}

func NewConnector(brokers []string) messaging.Connector {
	config := sarama.NewConfig()
	config.ClientID = "sample-client"
	config.Version = sarama.MaxVersion
	config.Producer.Compression = sarama.CompressionZSTD
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return &defaultConnector{config: config, brokers: brokers, mutex: &sync.Mutex{}}
}

func (this *defaultConnector) Connect(ctx context.Context) (messaging.Connection, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	client, err := sarama.NewClient(this.brokers, this.config)
	if err != nil {
		return nil, err
	}

	this.active = append(this.active, newConnection(client))
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
