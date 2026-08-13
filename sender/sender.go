package sender

const defaultMaxMessageSize = 500 * 1024 // 500KB
const defaultMaxQueueSize = 1000

func DefaultSenderOptions() SenderOptions {
	return SenderOptions{
		MaxMessageSize: defaultMaxMessageSize,
		MaxQueueSize:   defaultMaxQueueSize,
	}
}

type SenderOptions struct {
	MaxMessageSize int
	MaxQueueSize   int
}

func (o *SenderOptions) Fix() {
	if o.MaxMessageSize <= 0 {
		o.MaxMessageSize = defaultMaxMessageSize
	}
	if o.MaxQueueSize <= 0 {
		o.MaxQueueSize = defaultMaxQueueSize
	}
}

type Sender struct {
	ops SenderOptions
}

func New(opts SenderOptions) *Sender {
	opts.Fix()
	return &Sender{
		ops: opts,
	}
}
