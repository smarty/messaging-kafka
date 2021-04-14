package kafka

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/smartystreets/messaging/v3"
)

type defaultStream struct {
	logger        Logger
	monitor       Monitor
	messageTypes  map[uint32]string
	contentTypes  map[uint8]string
	reader        *kafka.Reader
	consumerGroup bool
	lifecycle     context.Context
	cancel        context.CancelFunc
}

func newStream(config configuration, reader *kafka.Reader, consumerGroup bool, parent context.Context) messaging.Stream {
	this := defaultStream{
		logger:        config.Logger,
		monitor:       config.Monitor,
		messageTypes:  config.MessageTypeIdentifiers,
		contentTypes:  config.ContentTypeIdentifiers,
		reader:        reader,
		consumerGroup: consumerGroup,
	}
	this.lifecycle, this.cancel = context.WithCancel(parent)
	go this.awaitCancel()
	return this
}
func (this defaultStream) awaitCancel() {
	<-this.lifecycle.Done()
	_ = this.reader.Close()
}

func (this defaultStream) Read(ctx context.Context, target *messaging.Delivery) error {
	raw, err := this.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	target.Upstream = raw
	target.Timestamp = raw.Time
	target.Topic = raw.Topic
	target.Partition = uint64(raw.Partition)
	target.DeliveryID = uint64(raw.Offset)
	target.Durable = true
	target.Payload = raw.Value
	target.Headers = computeHeaders2(raw.Headers)

	if this.populateMessageTypeAndContentType(raw.Value, target) {
		target.Payload = raw.Value[0:4]
	}

	return nil
}
func (this defaultStream) populateMessageTypeAndContentType(source []byte, target *messaging.Delivery) bool {
	if len(source) < 4 {
		target.MessageType = "unknown-message-type"
		target.ContentType = "unknown-content-type"
		return false
	}

	value := binary.LittleEndian.Uint32(source)
	messageTypeID := value >> 8
	contentTypeID := uint8(value << 24 >> 24)
	messageType, containsMessageType := this.messageTypes[messageTypeID]
	contentType, containsContentType := this.contentTypes[contentTypeID]

	if !containsMessageType {
		messageType = fmt.Sprintf("unknown-message-type-%d", messageTypeID)
	}
	if !containsContentType {
		contentType = fmt.Sprintf("unknown-content-type-%d", contentTypeID)
	}

	target.MessageType = messageType
	target.ContentType = contentType
	return true
}
func computeHeaders2(source []kafka.Header) map[string]interface{} {
	if len(source) == 0 {
		return nil
	}

	target := make(map[string]interface{}, len(source))

	for _, header := range source {
		target[header.Key] = string(header.Value)
	}

	return target
}

func (this defaultStream) Acknowledge(ctx context.Context, deliveries ...messaging.Delivery) error {
	if !this.consumerGroup {
		return nil
	}

	messages := make([]kafka.Message, 0, len(deliveries))
	for _, delivery := range deliveries {
		messages = append(messages, delivery.Upstream.(kafka.Message))
	}

	return this.reader.CommitMessages(ctx, messages...)
}

func (this defaultStream) Close() error {
	this.cancel()
	return nil
}
