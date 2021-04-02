package kafka

import (
	"context"
	"io"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultReader struct {
	config    configuration
	lifecycle context.Context
	cancel    func()
}

func newReader(config configuration, parent context.Context) messaging.Reader {
	this := defaultReader{config: config}
	this.lifecycle, this.cancel = context.WithCancel(parent)
	return this
}

func (this defaultReader) Stream(_ context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	reader, err := this.openReader(config)
	if err != nil {
		return nil, err
	}

	return newStream(this.config, reader, len(config.GroupName) > 0, this.lifecycle), nil
}
func (this defaultReader) openReader(config messaging.StreamConfig) (reader *kafka.Reader, err error) {
	kafkaConfig := kafka.ReaderConfig{
		Brokers:        this.config.Brokers,
		Logger:         this.config.DriverLogger,
		ErrorLogger:    this.config.DriverLogger,
		IsolationLevel: kafka.ReadCommitted,
		MaxBytes:       int(config.MaxMessageBytes),
	}

	if len(config.GroupName) == 0 {
		kafkaConfig.Topic = config.StreamName
		kafkaConfig.Partition = int(config.Partition)
	} else {
		kafkaConfig.GroupID = config.GroupName
		kafkaConfig.GroupTopics = config.Topics
		if len(config.StreamName) > 0 {
			kafkaConfig.GroupTopics = append(kafkaConfig.GroupTopics, config.StreamName)
		}
	}

	reader = kafka.NewReader(kafkaConfig)
	if config.Sequence > 0 && len(config.GroupName) == 0 {
		err = reader.SetOffset(int64(config.Sequence))
	}

	if err != nil {
		_ = reader.Close()
		reader = nil
	}

	return reader, err
}
func (this defaultReader) awaitCancel(resource io.Closer) {
	<-this.lifecycle.Done()
	_ = resource.Close()
}

func (this defaultReader) Close() error {
	this.cancel()
	return nil
}
