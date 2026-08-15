package sender

import (
	"context"
	"fmt"
	"sync"

	"github.com/anjankow/message-sender/sender/internal"
)

type Sender struct {
	ops   SenderOptions
	queue *internal.Queue
	stop  func()
}

// New creates a new Sender with the given options and starts processing the message queue.
// To stop processing, call the Stop function.
func New(opts SenderOptions) (*Sender, error) {
	if err := opts.validateAndFix(); err != nil {
		return nil, err
	}

	q, err := internal.NewQueue(opts.QueueCapacity)
	if err != nil {
		return nil, err
	}

	s := &Sender{
		ops:   opts,
		queue: q,
	}

	// Start background processing
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	// Define the stop function - it simply cancells the processing context
	// and waits for it to finish.
	s.stop = func() {
		cancel()
		wg.Wait()
	}
	s.startProcessing(ctx, &wg)
	return s, nil
}

// Send schedules a message to be sent.
func (s *Sender) Send(ctx context.Context, message string) error {
	if len(message) > s.ops.MaxMessageSize {
		return fmt.Errorf("message is too long: %d > %d", len(message), s.ops.MaxMessageSize)
	}

	if err := s.queue.Enqueue(ctx, message); err != nil {
		return err
	}
	return nil
}

// Stop stops the background processing of the sender.
func (s *Sender) Stop() {
	s.stop()
}

// startProcessing starts the background processing of the sender.
// It returns immediately when the passed context is done.
func (s *Sender) startProcessing(ctx context.Context, wg *sync.WaitGroup) {
	semaphore := make(chan struct{}, s.ops.ConcurrentRequests)
	client := internal.NewHTTPClient(s.ops.URL)

	wg.Go(func() {
		for {
			select {
			// Context is done when Stop is called - return immediatelly
			case <-ctx.Done():
				return
			default:
				msg, release, err := s.queue.Dequeue(ctx)
				if err != nil {
					// Handle the error as configured by the user
					s.ops.OnError(msg, err)
					continue
				}
				defer release()

				// Send the message in a separate goroutine,
				// use semaphore for the control over the number of concurrent requests.
				select {
				case <-ctx.Done():
					return
				case semaphore <- struct{}{}:
					wg.Go(func() {
						defer func() { <-semaphore }()

						if err := client.Post(ctx, msg); err != nil {
							s.ops.OnError(msg, err)
						}
					})
				}
			}
		}
	})
}
