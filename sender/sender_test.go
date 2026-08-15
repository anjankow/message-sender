package sender_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anjankow/message-sender/sender"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSender(t *testing.T) {
	t.Run("StartStop", func(t *testing.T) {
		// Check that the sender stops immediately.
		opts := sender.DefaultSenderOptions()
		opts.URL = "http://localhost:8080"
		s, err := sender.New(opts)
		require.NoError(t, err)

		stopped := make(chan struct{})
		go func() {
			s.Stop()
			stopped <- struct{}{}
		}()
		waitToRcv(t, stopped, time.Second)
	})
	t.Run("ProcessOneMessageSuccess", func(t *testing.T) {
		message := "My wonderful 資訊"
		responseWritten := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Assert method
			require.Equal(t, http.MethodPost, r.Method)
			// Assert correct request body
			b, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, message, string(b))
			w.WriteHeader(http.StatusNoContent)
			responseWritten <- struct{}{}
		}))
		defer srv.Close()

		opts := sender.DefaultSenderOptions()
		opts.URL = srv.URL
		opts.OnError = func(message bytes.Buffer, err error) {
			require.NoError(t, err)
		}

		s, err := sender.New(opts)
		require.NoError(t, err)
		defer s.Stop()

		require.NoError(t, s.Send(t.Context(), message))
		waitToRcv(t, responseWritten, time.Second)
	})
	t.Run("ProcessOneMessageFailure", func(t *testing.T) {
		message := "My wonderful 資訊"
		responseWritten := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			responseWritten <- struct{}{}
		}))
		defer srv.Close()

		opts := sender.DefaultSenderOptions()
		opts.URL = srv.URL
		// Assert the error can be accessed by the provided error callback
		opts.OnError = func(messageBuf bytes.Buffer, err error) {
			assert.ErrorIs(t, err, sender.HTTPError{Status: http.StatusInternalServerError})
			assert.Equal(t, message, messageBuf.String())
		}

		s, err := sender.New(opts)
		require.NoError(t, err)
		defer s.Stop()

		require.NoError(t, s.Send(t.Context(), message))
		waitToRcv(t, responseWritten, 2*time.Second)
	})
	t.Run("EmptyURL", func(t *testing.T) {
		opts := sender.DefaultSenderOptions()
		opts.URL = ""
		_, err := sender.New(opts)
		require.ErrorIs(t, err, sender.ErrURLRequired)
	})
}

func waitToRcv(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	tm := time.NewTimer(timeout)
	select {
	case <-ch:
		// Sleep here just one millisecond
		// to let the POST request be processed before we shutdown
		time.Sleep(time.Millisecond)
	case <-tm.C:
		t.Errorf("test timed out")
	}
}
