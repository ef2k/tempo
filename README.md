# tempo

[![Go
Reference](https://pkg.go.dev/badge/github.com/ef2k/tempo.svg)](https://pkg.go.dev/github.com/ef2k/tempo)
[![Go Report
Card](https://goreportcard.com/badge/github.com/ef2k/tempo)](https://goreportcard.com/report/github.com/ef2k/tempo)


Tempo is a thin buffer for high-frequency events. It collects incoming data and
emits it in batches instead of processing each event one at a time.

Think of Tempo as a waiting room for data: instead of sending every event the
instant it arrives, it holds items until enough are ready or the next batch is
scheduled.

This "batch with timeout" approach works best when events arrive quickly and
processing each one individually has high fixed overhead, like opening a
connection, making a system call, writing to a database, or calling an API.

What makes it a "thin" buffer is its architecture: its batching flow is driven
by a single dispatcher loop and Go channels, with no heavy mutex locking or
complex internal state machines. That helps avoid introducing heavier tools
like RabbitMQ or Kafka if all that's needed is in-process batching.

Use it for:
- Analytics and telemetry ingestion - Batch clicks, heartbeats, and errors
  before inserting them into a database or warehouse.
- External API batching - Group payloads into fewer requests to reduce
  overhead and help avoid rate limits.
- Agent and LLM event collection - Batch streamed thoughts, traces, and tool
  calls instead of reacting to every intermediate update in real time.

![3s demo](./examples/basic-3s-interval/tempo-3s.gif)

## Install
```sh
go get github.com/ef2k/tempo
```

## Documentation
[pkg.go.dev/github.com/ef2k/tempo](https://pkg.go.dev/github.com/ef2k/tempo)


## Sample Usage

See `examples/` for working code.

## Contribute

Improvements, fixes, and feedback are welcome.
