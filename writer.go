package kafka

import (
	"context"
	"encoding/binary"

	"github.com/segmentio/kafka-go"
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
			Compression:  computeCompressionMethod(config.CompressionMethod),
			Balancer:     computePartitionSelection(config.PartitionSelection),
			RequiredAcks: computeRequiredWrites(config.RequiredWrites),
			MaxAttempts:  int(config.MaxWriteAttempts),
			BatchSize:    int(config.MaxWriteBatchSize),
			BatchBytes:   0, // TODO: config value
			BatchTimeout: 0, // TODO: config value
			ReadTimeout:  0, // TODO: config value
			WriteTimeout: 0, // TODO: config value
			Async:        false,
			Completion:   nil,
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
		Time:  dispatch.Timestamp,
		Topic: dispatch.Topic,
		Key:   computeMessageKey(dispatch.Partition),
		Value: dispatch.Payload,
		Headers: []kafka.Header{
			// TODO: merge these fields into a numeric value
			{Key: "message-type", Value: []byte(dispatch.MessageType)},
			{Key: "content-type", Value: []byte(dispatch.ContentType)},
			{Key: "content-encoding", Value: []byte(dispatch.ContentEncoding)},
		},
	})
}
func computeMessageKey(partition uint64) []byte {
	if partition == 0 {
		return nil
	}

	target := make([]byte, 8)
	binary.LittleEndian.PutUint64(target, partition)
	return target
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
