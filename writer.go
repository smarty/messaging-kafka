package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"github.com/smartystreets/messaging/v3"
)

type defaultWriter struct {
	config    configuration
	writer    *kafka.Writer
	lifecycle context.Context
	cancel    func()
	pending   []kafka.Message
}

func newWriter(config configuration, parent context.Context) messaging.CommitWriter {
	this := &defaultWriter{
		config: config,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(config.Brokers...),
			Balancer:     nil,              // TODO: config value
			MaxAttempts:  1,                // TODO: config value
			BatchSize:    4096,             // TODO: config value
			BatchBytes:   1024 * 1024 * 16, // TODO: config value
			BatchTimeout: 0,                // TODO: config value
			ReadTimeout:  0,                // TODO: config value
			WriteTimeout: 0,                // TODO: config value
			RequiredAcks: kafka.RequireOne, // TODO: config value// durability
			Async:        false,
			Completion:   nil,
			Compression:  compress.Lz4, // TODO: config value
			Logger:       config.Logger,
			ErrorLogger:  config.Logger,
			Transport:    nil,
		},
	}

	this.lifecycle, this.cancel = context.WithCancel(parent)
	go func() {
		<-this.lifecycle.Done()
		_ = this.writer.Close()
	}()

	return this
}

func (this *defaultWriter) Write(_ context.Context, dispatches ...messaging.Dispatch) (int, error) {
	for _, dispatch := range dispatches {
		this.write(dispatch)
	}

	return len(dispatches), nil
}
func (this *defaultWriter) write(dispatch messaging.Dispatch) {
	this.pending = append(this.pending, kafka.Message{
		Topic:   dispatch.Topic,
		Key:     nil, // TODO
		Value:   dispatch.Payload,
		Headers: nil,
		Time:    dispatch.Timestamp,
	})
}

func (this *defaultWriter) Commit() error {
	err := this.writer.WriteMessages(this.lifecycle, this.pending...)
	this.pending = this.pending[0:0]
	return err
}
func (this *defaultWriter) Rollback() error {
	this.pending = this.pending[0:0]
	return nil
}

func (this *defaultWriter) Close() error {
	this.cancel()
	return nil
}
