package internal

import (
	"bytes"
	"context"
	"fmt"
)

const estimatedMessageSize = 1024 // 1KB

// Queue implements a thread safe FIFO queue.
type Queue struct {
	pool    MemPool
	buffers chan *bytes.Buffer
}

func NewQueue(capacity int) (*Queue, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("invalid capacity: %d, must be > 0", capacity)
	}
	return &Queue{
		pool:    NewMemPool(),
		buffers: make(chan *bytes.Buffer, capacity),
	}, nil
}

// Enqueue adds a message to the end of the queue.
// This operation is blocking if the queue capacity is exceeded.
// In such case cancel the context to return early.
func (q *Queue) Enqueue(ctx context.Context, message string) error {
	buf := q.pool.GetBuffer()
	if n, err := buf.WriteString(message); err != nil {
		return fmt.Errorf("failed to write string to buffer: %w", err)
	} else if n != len(message) {
		return fmt.Errorf("failed to write string to buffer: wrote %d bytes, expected %d", n, len(message))
	}

	select {
	case q.buffers <- buf:
	case <-ctx.Done():
		return fmt.Errorf("failed to enqueue: %w", ctx.Err())
	}
	return nil
}

// Dequeue removes and returns the first message from the queue.
// This operation is blocking if the queue is empty.
// In such case cancel the context to return early.
// The release function must be called to return the buffer to the pool when the message is no longer needed.
func (q *Queue) Dequeue(ctx context.Context) (message *bytes.Buffer, release func(), err error) {
	select {
	case buf := <-q.buffers:
		return buf, func() { q.release(buf) }, nil
	case <-ctx.Done():
		return nil, func() {}, fmt.Errorf("failed to dequeue: %w", ctx.Err())
	}
}

func (q *Queue) release(buf *bytes.Buffer) {
	if buf.Cap() > estimatedMessageSize {
		// We don't want to keep large buffers in memory,
		// so we don't return it to the pool and let the GC remove it.
		return
	}
	q.pool.ReleaseBuffer(buf)
}
