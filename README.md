# tempo

[![Go
Reference](https://pkg.go.dev/badge/github.com/ef2k/tempo.svg)](https://pkg.go.dev/github.com/ef2k/tempo)
[![Go Report
Card](https://goreportcard.com/badge/github.com/ef2k/tempo)](https://goreportcard.com/report/github.com/ef2k/tempo)


Tempo is a thin buffer that collects high-frequency events so they can be
processed in batches instead of one at a time.

Think of Tempo as a waiting room for data, instead of sending every piece of
information the instance it arrives, it collects it until a certain amount is
collected or when the next batch is scheduled.

This approach is called "batch with timeout" and its most useful when you have
high-frequency events with high-overhead. In other words, when you have a fire
hose of incoming data and the way you process them (opening a connection, making
a system call, writing to a database, calling an API)  is more expensive than
the data itself.

What makes it a "thin" buffer is its architecture: it’s built entirely on Go
channels with no heavy mutex locking or complex internal state machines. This
makes it very performant for high-concurrency environments.

Good use cases:
- Analytics / telemetry ingestion
- Collecting payloads before calling an external API 
- Embedding or vector writes
- Agent/LLM event collection

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
