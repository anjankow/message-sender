package sender_test

import (
	"testing"
	"time"

	"github.com/anjankow/message-sender/sender"
	"github.com/stretchr/testify/require"
)

func TestSender(t *testing.T) {
	t.Run("StartStop", func(t *testing.T) {
		// Check that the sender stops immediately.
		opts := sender.DefaultSenderOptions()
		opts.URL = "http://localhost:8080"
		tm := time.NewTimer(time.Millisecond * 100)
		s, err := sender.New(opts)
		require.NoError(t, err)

		stopped := make(chan struct{})
		go func() {
			s.Stop()
			stopped <- struct{}{}
		}()
		select {
		case <-stopped:
		case <-tm.C:
			t.Errorf("stopping the sender took too long")
		}
	})
}
