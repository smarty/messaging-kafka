package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/smartystreets/messaging/v3"
	"strconv"
)

type defaultWriter struct {
	buffer   []*sarama.ProducerMessage
	producer sarama.SyncProducer
}

func newWriter(producer sarama.SyncProducer) messaging.CommitWriter {
	return &defaultWriter{producer: producer}
}

func (this *defaultWriter) Write(ctx context.Context, dispatches ...messaging.Dispatch) (int, error) {
	for _, dispatch := range dispatches {
		this.buffer = append(this.buffer, &sarama.ProducerMessage{
			Topic:     dispatch.Topic,
			Key:       nil, // TODO
			Value:     sarama.ByteEncoder(dispatch.Payload),
			Headers:   buildRecordHeaders(dispatch),
			Partition: int32(dispatch.Partition),
			Timestamp: dispatch.Timestamp,
		})
	}

	return len(dispatches), nil
}
func buildRecordHeaders(dispatch messaging.Dispatch) (values []sarama.RecordHeader) {
	// TODO: DON'T do it this way. The behavior herein is a quick/temp hack to get things running

	if dispatch.SourceID > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("source-id"),
			Value: []byte(strconv.FormatUint(dispatch.SourceID, 10)),
		})
	}
	if dispatch.MessageID > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("message-id"),
			Value: []byte(strconv.FormatUint(dispatch.MessageID, 10)),
		})
	}
	if dispatch.CorrelationID > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("correlation-id"),
			Value: []byte(strconv.FormatUint(dispatch.CorrelationID, 10)),
		})
	}
	if len(dispatch.MessageType) > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("message-type"),
			Value: []byte(dispatch.MessageType),
		})
	}
	if len(dispatch.ContentType) > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("content-type"),
			Value: []byte(dispatch.ContentType),
		})
	}
	if len(dispatch.ContentEncoding) > 0 {
		values = append(values, sarama.RecordHeader{
			Key:   []byte("content-encoding"),
			Value: []byte(dispatch.ContentEncoding),
		})
	}

	return values
}

func (this *defaultWriter) Commit() error {
	return this.producer.SendMessages(this.buffer)
}
func (this *defaultWriter) Rollback() error {
	this.buffer = this.buffer[0:0]
	return nil
}

func (this *defaultWriter) Close() error {
	return this.producer.Close()
}
