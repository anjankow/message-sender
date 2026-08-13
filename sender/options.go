package sender

import (
	"bytes"
	"errors"
	"log/slog"
)

var ErrURLRequired = errors.New("URL is required")

const defaultMaxMessageSize = 500 * 1024 // 500KB
const defaultQueueCapacity = 1000
const defaultConcurrentReqs = 10

type SenderOptions struct {
	// URL is the URL of the message sender service. REQUIRED
	URL string
	// MaxMessageSize is the maximum size of a message in bytes. If it's <= 0, the default value is used.
	MaxMessageSize int
	// QueueCapacity is the maximum number of messages that can be queued for sending without blocking. If it's <= 0, the default value is used.
	QueueCapacity int
	// ConcurrentRequests is the maximum number of concurrent HTTP requests. If it's <= 0, the default value is used.
	ConcurrentRequests int
	// OnError is the function called when an error occurs while processing and sending a message.
	// By default, it logs the message and the error.
	OnError func(message bytes.Buffer, err error)
}

func DefaultSenderOptions() SenderOptions {
	return SenderOptions{
		MaxMessageSize:     defaultMaxMessageSize,
		QueueCapacity:      defaultQueueCapacity,
		ConcurrentRequests: defaultConcurrentReqs,
		OnError:            logError,
	}
}

func (o *SenderOptions) validateAndFix() error {
	if o.URL == "" {
		return ErrURLRequired
	}

	if o.MaxMessageSize <= 0 {
		o.MaxMessageSize = defaultMaxMessageSize
	}
	if o.QueueCapacity <= 0 {
		o.QueueCapacity = defaultQueueCapacity
	}
	if o.ConcurrentRequests <= 0 {
		o.ConcurrentRequests = defaultConcurrentReqs
	}
	if o.OnError == nil {
		o.OnError = logError
	}
	return nil
}

func logError(message bytes.Buffer, err error) {
	// Messages can be very large, so we only log the first 200 bytes
	msgHead := string(message.Bytes()[:min(200, len(message.Bytes()))])
	slog.Error("Failed to send a message",
		slog.Any("err", err),
		slog.String("message", msgHead),
		slog.Int("length", message.Len()),
	)
}
