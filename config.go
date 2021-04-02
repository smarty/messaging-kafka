package kafka

import (
	"context"
	"math"
	"time"

	"github.com/segmentio/kafka-go"
)

type configuration struct {
	Brokers            []string
	CompressionMethod  compressionMethod
	PartitionSelection partitionSelection
	RequiredWrites     requiredWrites
	MaxWriteAttempts   uint8
	MaxWriteBatchSize  uint16
	BatchWriteInterval time.Duration
	Context            context.Context
	Monitor            Monitor
	Logger             Logger
	DriverLogger       Logger
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Brokers(value ...string) option {
	return func(this *configuration) { this.Brokers = value }
}
func (singleton) CompressionMethod(value compressionMethod) option {
	return func(this *configuration) { this.CompressionMethod = value }
}
func (singleton) PartitionSelection(value partitionSelection) option {
	return func(this *configuration) { this.PartitionSelection = value }
}
func (singleton) RequiredWrites(value requiredWrites) option {
	return func(this *configuration) { this.RequiredWrites = value }
}
func (singleton) MaxWriteAttempts(value uint8) option {
	return func(this *configuration) { this.MaxWriteAttempts = value }
}
func (singleton) MaxWriteBatchSize(value uint16) option {
	return func(this *configuration) { this.MaxWriteBatchSize = value }
}
func (singleton) BatchWriteInterval(value time.Duration) option {
	return func(this *configuration) { this.BatchWriteInterval = value }
}

func (singleton) Context(value context.Context) option {
	return func(this *configuration) { this.Context = value }
}
func (singleton) Logger(value Logger) option {
	return func(this *configuration) { this.Logger = value }
}
func (singleton) Monitor(value Monitor) option {
	return func(this *configuration) { this.Monitor = value }
}
func (singleton) DriverLogger(value Logger) option {
	return func(this *configuration) { this.DriverLogger = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	return append([]option{
		Options.Brokers("127.0.0.1:9092"),
		Options.CompressionMethod(CompressionMethodLz4),
		Options.PartitionSelection(PartitionSelectionRoundRobin),
		Options.RequiredWrites(RequiredWritesOne),
		Options.MaxWriteAttempts(1),
		Options.MaxWriteBatchSize(math.MaxUint16),
		Options.BatchWriteInterval(time.Millisecond),
		Options.Context(context.Background()),
		Options.Logger(nop{}),
		Options.Monitor(nop{}),
	}, options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type nop struct{}

func (nop) Printf(string, ...interface{}) {}

func (nop) ConnectionOpened(error)             {}
func (nop) ConnectionClosed()                  {}
func (nop) DispatchPublished()                 {}
func (nop) DeliveryReceived()                  {}
func (nop) DeliveryAcknowledged(uint64, error) {}
func (nop) TransactionCommitted(error)         {}
func (nop) TransactionRolledBack(error)        {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Logger interface {
	Printf(string, ...interface{})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Monitor interface {
	ConnectionOpened(error)
	ConnectionClosed()
	DispatchPublished()
	DeliveryReceived()
	DeliveryAcknowledged(uint64, error)
	TransactionCommitted(error)
	TransactionRolledBack(error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type compressionMethod uint8

var (
	CompressionMethodNone      compressionMethod = 0
	CompressionMethodGZip      compressionMethod = 1
	CompressionMethodLz4       compressionMethod = 2
	CompressionMethodSnappy    compressionMethod = 3
	CompressionMethodZStandard compressionMethod = 4
)

func computeCompressionMethod(method compressionMethod) kafka.Compression {
	switch method {
	case CompressionMethodGZip:
		return kafka.Gzip
	case CompressionMethodLz4:
		return kafka.Lz4
	case CompressionMethodSnappy:
		return kafka.Snappy
	case CompressionMethodZStandard:
		return kafka.Zstd
	case CompressionMethodNone:
		return 0
	default:
		panic("unknown compression method value")
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type partitionSelection uint8

var (
	PartitionSelectionRoundRobin partitionSelection = 0
	PartitionSelectionMurmurHash partitionSelection = 1
	PartitionSelectionFNVHash    partitionSelection = 2
	PartitionSelectionCrc32Hash  partitionSelection = 3
	PartitionSelectionLeastBytes partitionSelection = 4
)

func computePartitionSelection(method partitionSelection) kafka.Balancer {
	switch method {
	case PartitionSelectionRoundRobin:
		return &kafka.RoundRobin{}
	case PartitionSelectionMurmurHash:
		return kafka.Murmur2Balancer{}
	case PartitionSelectionFNVHash:
		return &kafka.Hash{}
	case PartitionSelectionCrc32Hash:
		return kafka.CRC32Balancer{}
	case PartitionSelectionLeastBytes:
		return &kafka.LeastBytes{}
	default:
		panic("unknown partition selection method value")
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type requiredWrites uint8

var (
	RequiredWritesNone requiredWrites = 0
	RequiredWritesOne  requiredWrites = 1
	RequiredWritesAll  requiredWrites = 2
)

func computeRequiredWrites(required requiredWrites) kafka.RequiredAcks {
	switch required {
	case RequiredWritesOne:
		return kafka.RequireOne
	case RequiredWritesAll:
		return kafka.RequireAll
	case RequiredWritesNone:
		return kafka.RequireNone
	default:
		panic("unknown required acknowledgement value")
	}
}
