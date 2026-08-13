package internal

import (
	"bytes"
	"context"
	"fmt"
)

// Queue implements a thread safe FIFO queue.
type Queue struct {
	pool    *MemPool
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
	buf.WriteString(message)
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
func (q *Queue) Dequeue(ctx context.Context) (string, error) {
	select {
	case buf := <-q.buffers:
		return buf.String(), nil
	case <-ctx.Done():
		return "", fmt.Errorf("failed to dequeue: %w", ctx.Err())
	}
}
