package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/smarty/messaging/v4"
)

type defaultReader struct {
	config    configuration
	logger    Logger
	monitor   Monitor
	lifecycle context.Context
	cancel    context.CancelFunc
}

func newReader(config configuration, parent context.Context) messaging.Reader {
	this := defaultReader{
		config:  config,
		logger:  config.Logger,
		monitor: config.Monitor,
	}
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

func (this defaultReader) Close() error {
	this.cancel()
	return nil
}
