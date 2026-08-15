package internal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anjankow/message-sender/sender/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	t.Run("InvalidCapacity", func(t *testing.T) {
		_, err := internal.NewQueue(-1)
		require.Error(t, err)
		_, err = internal.NewQueue(0)
		require.Error(t, err)
	})
	t.Run("FIFO", func(t *testing.T) {
		ctx := t.Context()
		q, err := internal.NewQueue(3)
		require.NoError(t, err)

		// Enqueue three messages
		require.NoError(t, q.Enqueue(ctx, "a"))
		require.NoError(t, q.Enqueue(ctx, "b"))
		require.NoError(t, q.Enqueue(ctx, "c"))

		// Assert that the messages are dequeued in the correct order
		a, _, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.Equal(t, "a", a.String())
		b, _, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.Equal(t, "b", b.String())
		c, _, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.Equal(t, "c", c.String())
	})
	t.Run("MaxCapacityExceeded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		q, err := internal.NewQueue(2)
		require.NoError(t, err)
		require.NoError(t, q.Enqueue(ctx, "a"))
		require.NoError(t, q.Enqueue(ctx, "b"))

		enqueueDone := make(chan struct{}, 1)
		var wg sync.WaitGroup
		wg.Go(func() {
			err := q.Enqueue(ctx, "c")
			enqueueDone <- struct{}{}
			assert.Error(t, err, context.Canceled)
		})

		// Assert that Enqueue blocks when max capacity is exceeded
		// and continue after 100 ms to
		select {
		case <-enqueueDone:
			wg.Wait()
			t.Errorf("expected to block on Enqueue when max capacity exceeded")
		case <-time.After(100 * time.Millisecond): // sufficient time for Enqueue to be called
		}

		// Now cancel the context
		cancel()
		wg.Wait()
	})
	t.Run("Multithreaded", func(t *testing.T) {
		ctx := t.Context()
		capacity := 200
		q, err := internal.NewQueue(capacity)
		require.NoError(t, err)

		start := make(chan struct{})

		var wg sync.WaitGroup
		for range capacity * 2 {
			wg.Go(func() {
				<-start
				err := q.Enqueue(ctx, "a")
				assert.NoError(t, err)
			})

			wg.Go(func() {
				<-start
				d, release, err := q.Dequeue(ctx)
				defer release()
				assert.NoError(t, err)
				assert.Equal(t, "a", d.String())

			})
		}

		// Start at the same moment
		close(start)
		wg.Wait()
	})
}
