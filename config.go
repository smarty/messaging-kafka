package kafka

import "context"

type configuration struct {
	Brokers []string
	Context context.Context
	Logger  Logger
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Brokers(value ...string) option {
	return func(this *configuration) { this.Brokers = value }
}

func (singleton) Context(value context.Context) option {
	return func(this *configuration) { this.Context = value }
}

func (singleton) Logger(value Logger) option {
	return func(this *configuration) { this.Logger = value }
}

func (singleton) apply(options ...option) option {
	return func(this *configuration) {
		for _, option := range Options.defaults(options...) {
			option(this)
		}
	}
}
func (singleton) defaults(options ...option) []option {
	var defaultLogger = nop{}

	return append([]option{
		Options.Brokers("127.0.0.1:2181"),
		Options.Context(context.Background()),
		Options.Logger(defaultLogger),
	}, options...)
}

type nop struct{}

func (nop) Printf(string, ...interface{}) {}

//////

type Logger interface {
	Printf(string, ...interface{})
}
