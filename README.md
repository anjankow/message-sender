# message-sender

A Go library to facilitate sending sudden spikes of messages to a target URL.

## What it does

You hand it messages, and it turns each one into an HTTP `POST` request against the provided URL with the message as the request body. 
The `Send` call is not blocking, the messages are silently handled in the background.

On top of the library, this repo ships a small executable (see cmd/main.go) that uses the library to send messages from stdin at a configurable interval.

## Usage

### Library

```go
// see sender.SenderOptions for all tunable fields
opts := sender.DefaultSenderOptions()
// target URL accepting POST requests
opts.URL = "https://example.com/notifications"

s, err := sender.New(opts)
if err != nil {
	panic(err)
}

// non-blocking; enqueues and returns immediately
if err := s.Send(ctx, "try me"); err != nil {
	panic(err)
}

// stops all background workers and ongoing requests
s.Stop() 
```
After calling `Stop()`, the sender will not send any new messages.
To restart the sender, create a new instance with `New(opts)`.

### CLI

The executable reads messages from stdin (one per line) and sends them via the library on a configurable interval.

To try it out before using the actual URL, pass the `-test` flag.
The flag will spin up a local test server on `:7777` printing each message it receives.

```bash
# send each line from stdin to a target URL every second (default interval)
go run ./cmd/main.go -url=https://example.com/notifications

# custom interval (in milliseconds)
go run ./cmd/main.go -url=https://example.com/notifications -interval=500

# spin up a local test server on :7777 to try things out without a real endpoint
go run ./cmd/main.go -test -url=http://localhost:7777

# set verbosity to INFO
go run ./cmd/main.go -url=https://example.com/notifications -log-level=0
```

Then type messages (or pipe a file):

```bash
echo -e "message one\nmessage two\nmessage three" | go run ./cmd/main.go -test -url=http://localhost:7777 
```

**Flags:**

| Flag | Type | Default | Description |
|---|---|---|---|
| `-url` | string | `""` | Target URL that messages are POSTed to. Required. |
| `-interval` | int | `1000` | Interval, in milliseconds, between reading and sending successive messages from stdin. |
| `-test` | bool | `false` | Starts a local test HTTP server on port `7777` that accepts and logs incoming POST requests, useful for trying out the tool without a real endpoint. |
| `-log-level` | int | `int(slog.LevelDebug)` | Log verbosity, using [`slog`](https://pkg.go.dev/log/slog) level values (e.g. `-4` debug, `0` info, `4` warn, `8` error). |

Press `Ctrl+C` (`SIGINT`) at any time to trigger graceful shutdown.
If you input messages directly via stdin, press `Enter` to stop scanning for messages after.
