package kafka

type configuration struct {
	Brokers []string
	Logger  Logger
}

var Options singleton

type singleton struct{}
type option func(*configuration)

func (singleton) Brokers(value ...string) option {
	return func(this *configuration) { this.Brokers = value }
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
		Options.Logger(defaultLogger),
	}, options...)
}

type nop struct{}

func (nop) Printf(...interface{}) {}

//////

type Logger interface {
	Printf(...interface{})
}
