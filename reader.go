package kafka

import (
	"context"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultReader struct {
	config configuration
	active []io.Closer
	mutex  *sync.Mutex
}

func newReader(config configuration) messaging.Reader {
	return &defaultReader{config: config}
}

func (this *defaultReader) Stream(_ context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	reader, err := this.openReader(config)
	if err != nil {
		return nil, err
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()
	stream := newStream(this.config, reader)
	this.active = append(this.active, stream)
	return stream, nil
}
func (this *defaultReader) openReader(config messaging.StreamConfig) (reader *kafka.Reader, err error) {
	kafkaConfig := kafka.ReaderConfig{
		Brokers:     this.config.Brokers,
		Topic:       config.StreamName,
		Partition:   int(config.Partition),
		StartOffset: int64(config.Sequence), // consumer groups
	}

	defer func() { err = recover().(error) }()
	reader = kafka.NewReader(kafkaConfig)
	if config.Sequence > 0 {
		err = reader.SetOffset(int64(config.Sequence))
	}

	return reader, err
}

func (this *defaultReader) Close() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for i := range this.active {
		_ = this.active[i].Close()
		this.active[i] = nil
	}
	this.active = this.active[0:0]

	return nil
}
