module github.com/smartystreets/messaging-kafka

go 1.16

require (
	github.com/segmentio/kafka-go v0.4.12
	github.com/smartystreets/messaging/v3 v3.2.1
)

replace (
	github.com/smartystreets/messaging/v3 v3.2.1 => ../messaging/
)
