package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultWriter struct {
	messageTypes  map[string]uint32
	contentTypes  map[string]uint8
	transactional bool
	writer        *kafka.Writer
	lifecycle     context.Context
	cancel        context.CancelFunc
	pending       []kafka.Message
}

func newWriter(config configuration, parent context.Context, transactional bool) messaging.CommitWriter {
	this := &defaultWriter{
		messageTypes:  config.messageTypeIdentifiers,
		contentTypes:  config.contentTypeIdentifiers,
		transactional: transactional,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(config.Brokers...),
			Compression:  computeCompressionMethod(config.CompressionMethod),
			Balancer:     computePartitionSelection(config.PartitionSelection),
			RequiredAcks: computeRequiredWrites(config.RequiredWrites),
			MaxAttempts:  int(config.MaxWriteAttempts),
			BatchSize:    int(config.MaxWriteBatchSize),
			BatchTimeout: config.BatchWriteInterval,
			Async:        false,
			Logger:       config.DriverLogger,
			ErrorLogger:  config.DriverLogger,
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
	for i, dispatch := range dispatches {
		if len(dispatch.Topic) == 0 {
			return i, messaging.ErrEmptyDispatchTopic
		}

		this.pending = append(this.pending, this.newMessage(dispatch))
	}

	if !this.transactional {
		return len(dispatches), nil
	}

	if err := this.Commit(); err != nil {
		return 0, err
	}

	return len(dispatches), nil
}
func (this *defaultWriter) newMessage(dispatch messaging.Dispatch) kafka.Message {
	targetHeaders := make([]kafka.Header, 0, len(dispatch.Headers)+1)
	targetHeaders = append(targetHeaders, kafka.Header{
		Key:   messageTypeHeaderName,
		Value: this.computeMessageType(dispatch.MessageType, dispatch.ContentType),
	})

	for key, value := range dispatch.Headers {
		targetHeaders = append(targetHeaders, kafka.Header{
			Key:   key,
			Value: []byte(fmt.Sprint(value)),
		})
	}

	return kafka.Message{
		Time:    dispatch.Timestamp,
		Topic:   dispatch.Topic,
		Key:     computeMessageKey(dispatch.Partition),
		Value:   dispatch.Payload,
		Headers: targetHeaders,
	}
}
func (this *defaultWriter) computeMessageType(messageType, contentType string) []byte {
	value := this.messageTypes[messageType] << 8
	value += uint32(this.contentTypes[contentType])
	target := make([]byte, 4)
	binary.LittleEndian.PutUint32(target, value)
	return target
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
