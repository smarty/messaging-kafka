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
	stream := newStream(this.config, reader, len(config.GroupName) > 0)
	this.active = append(this.active, stream)
	return stream, nil
}
func (this *defaultReader) openReader(config messaging.StreamConfig) (reader *kafka.Reader, err error) {
	kafkaConfig := kafka.ReaderConfig{
		Brokers:        this.config.Brokers,
		Logger:         this.config.Logger,
		ErrorLogger:    this.config.Logger,
		IsolationLevel: kafka.ReadCommitted,
		MaxBytes:       int(config.MaxMessageBytes),
	}

	if len(config.GroupName) == 0 {
		kafkaConfig.Topic = config.StreamName
		kafkaConfig.Partition = int(config.Partition)
	} else {
		kafkaConfig.GroupID = config.GroupName
		kafkaConfig.GroupTopics = config.Topics
	}

	reader = kafka.NewReader(kafkaConfig)
	if config.Sequence > 0 && len(config.GroupName) > 0 {
		err = reader.SetOffset(int64(config.Sequence))
	}

	if err != nil {
		_ = reader.Close()
		reader = nil
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
