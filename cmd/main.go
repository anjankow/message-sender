package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/anjankow/message-sender/sender"
)

var intervalFlag = flag.Int("interval", 1000, "interval in milliseconds")
var urlFlag = flag.String("url", "", "URL to send the message to")
var testFlag = flag.Bool("test", false, "run a test server on port 7777")
var logLevelFlag = flag.Int("log-level", int(slog.LevelDebug), "log level as defined by slog")

func main() {
	flag.Parse()
	slog.SetLogLoggerLevel(slog.Level(*logLevelFlag))

	slog.Info("Starting the message sender...", "interval", *intervalFlag, "url", *urlFlag, "test", *testFlag)

	if *intervalFlag <= 0 {
		slog.Error("Interval must be greater than 0")
		return
	}
	interval := time.Duration(*intervalFlag) * time.Millisecond

	// Create and start the sender
	opts := sender.DefaultSenderOptions()
	opts.URL = *urlFlag
	s, err := sender.New(opts)
	if err != nil {
		slog.Error("Failed to create a sender", "error", err)
		return
	}
	slog.Info("Sender started")

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Run a test server if test flag is set
	if *testFlag {
		runTestServer(ctx, &wg)
	}

	// Scan the messages
	messages := make(chan string, 500)
	wg.Go(func() {
		defer slog.Debug("Scanner finished")
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				line := scanner.Text()
				messages <- line
			}
		}
	})

	// Send the messages
	wg.Go(func() {
		defer slog.Debug("Sending finished")
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.Tick(interval):
				// We don't have to assure the exact number of messages sent at this moment,
				// so we just get the channel length for this instant without blocking it
				currentLen := len(messages)
				for range currentLen {
					msg := <-messages
					if err := s.Send(ctx, msg); err != nil {
						slog.Error("Failed to send message", "message", msg, "error", err)
						if errors.Is(err, context.Canceled) {
							return
						}
					}
				}
				slog.Debug("Messages sent", "count", currentLen)
			}
		}
	})

	// Wait for signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("Signal received")

	s.Stop()
	slog.Debug("Sender stopped")
	cancel()

	wg.Wait()
	slog.Info("DONE")
}

func runTestServer(ctx context.Context, wg *sync.WaitGroup) {
	srv := &http.Server{
		Addr: ":7777",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				slog.Error("Failed to read request body", "error", err)
				return
			}
			fmt.Println("-- Message: ", string(b))
			w.WriteHeader(http.StatusNoContent)
		}),
	}

	wg.Go(func() {
		defer slog.Debug("Test server finished")
		wg.Go(func() {
			if err := srv.ListenAndServe(); err != nil {
				slog.Error("Failed to start test server", "error", err)
			}
		})
		<-ctx.Done()
		_ = srv.Shutdown(ctx)
	})
}
