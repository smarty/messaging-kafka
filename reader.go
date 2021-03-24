package kafka

import (
	"context"
	"github.com/Shopify/sarama"

	"github.com/smartystreets/messaging/v3"
)

type defaultReader struct {
	consumer sarama.Consumer
}

func newReader(consumer sarama.Consumer) messaging.Reader {
	return defaultReader{consumer: consumer}
}

func (this defaultReader) Stream(ctx context.Context, config messaging.StreamConfig) (messaging.Stream, error) {
	stream, err := this.consumer.ConsumePartition(config.StreamName, int32(config.Partition), int64(config.Sequence))
	if err != nil {
		return nil, err
	}

	return newStream(stream), nil
}

func (this defaultReader) Close() error {
	return this.consumer.Close()
}
