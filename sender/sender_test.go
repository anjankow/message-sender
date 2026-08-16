package sender_test

import (
	"bytes"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
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
		waitToRcv(t, stopped, time.Millisecond)
	})

	t.Run("ProcessMessageSuccess", func(t *testing.T) {
		tcs := []struct {
			name string
			msg  string
		}{
			{"Empty", ""},
			{"OneByte", "1"},
			{"MoreBytes", "My wonderful 資訊"},
		}
		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				message := tc.msg
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
				opts.OnError = func(message *bytes.Buffer, err error) {
					require.NoError(t, err)
				}

				s, err := sender.New(opts)
				require.NoError(t, err)
				defer s.Stop()

				require.NoError(t, s.Send(t.Context(), message))
				waitToRcv(t, responseWritten, time.Second)
			})
		}
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
		opts.OnError = func(messageBuf *bytes.Buffer, err error) {
			assert.ErrorIs(t, err, sender.HTTPError{Status: http.StatusInternalServerError})
		}

		s, err := sender.New(opts)
		require.NoError(t, err)
		defer s.Stop()

		require.NoError(t, s.Send(t.Context(), message))
		waitToRcv(t, responseWritten, time.Second)
	})
	t.Run("MultithreadedHighVolume", func(t *testing.T) {
		var count atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Confirm just the first byte of the body
			var buf [1]byte
			_, err := r.Body.Read(buf[:])
			require.NoError(t, err)
			defer r.Body.Close()
			assert.Equal(t, byte(r.ContentLength), buf[0])

			w.WriteHeader(http.StatusOK)
			cnt := count.Add(1)
			t.Logf("Received request: num %v (len %v)", cnt, r.ContentLength)
		}))
		defer srv.Close()

		opts := sender.DefaultSenderOptions()
		opts.URL = srv.URL
		opts.ConcurrentRequests = 100
		opts.OnError = func(message *bytes.Buffer, err error) {
			require.NoErrorf(t, err, "message: %s", message.String())
		}
		s, err := sender.New(opts)
		require.NoError(t, err)
		defer s.Stop()

		const msgCnt = 100000
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range msgCnt {
			n := rand.Intn(254) + 2
			message := bytes.Repeat([]byte{byte(n)}, n)

			wg.Add(1)
			go func(msg string) {
				defer wg.Done()
				<-start
				require.NoError(t, s.Send(t.Context(), msg))
			}(string(message))
		}
		close(start)
		wg.Wait()
		t.Logf("All messages scheduled for sending (sent %v out of %v)", count.Load(), msgCnt)

		counterReached := make(chan struct{})
		go func() {
			tm := time.Tick(time.Second)
			for range tm {
				if x := count.Load(); x >= int32(msgCnt) {
					t.Logf("Counter reached: %d/%d", x, msgCnt)
					counterReached <- struct{}{}
					return
				}
			}
		}()
		tm := time.NewTimer(15 * time.Second)
		select {
		case <-counterReached:
			time.Sleep(time.Second)
		case <-tm.C:
			t.Errorf("test timed out, counter: %d", count.Load())
		}
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
