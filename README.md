# tempo

[![Go Reference](https://pkg.go.dev/badge/github.com/ef2k/tempo.svg)](https://pkg.go.dev/github.com/ef2k/tempo)
[![Go Report Card](https://goreportcard.com/badge/github.com/ef2k/tempo)](https://goreportcard.com/report/github.com/ef2k/tempo)

A channel-based batch dispatcher that emits items either when the interval expires or when the batch size limit is reached, whichever comes first.

## Features
- **Batching by periodic time intervals** <br> Set a time interval and receive accumulated items when the interval expires.

- **Dispatching as soon as a batch limit is met** <br> If a batch fills before the interval expires, it is dispatched immediately.

- **Channel-driven design** <br> The dispatcher is built around standard Go channels and timer-based scheduling.

- **Works with arbitrary values** <br> Items are queued as `interface{}` values and emitted in batches.

## Current Behavior
- Batches are delivered on `Dispatcher.Batch` either when `MaxBatchItems` is reached or when `Interval` expires, whichever happens first.

- The dispatcher coordinates producers and consumers through Go channels and timer-based scheduling.

- With multiple producer goroutines, `tempo` preserves the order items are received by the dispatcher. It does not establish a strict global ordering across independent producers beyond normal channel receive order.

![3s demo](./examples/basic-3s-interval/tempo-3s.gif)

## Install
```sh
go get github.com/ef2k/tempo
```

## Documentation
[pkg.go.dev/github.com/ef2k/tempo](https://pkg.go.dev/github.com/ef2k/tempo)

## Test
```sh
go test ./...
```

## Sample Usage

Dispatch a batch at 10 second intervals or as soon as a batching limit of 50 items is met.
See `examples/` for working code.

```go
// initialize
d := tempo.NewDispatcher(&tempo.Config{
  Interval:      time.Duration(10) * time.Second,
  MaxBatchItems: 50,
})
defer d.Stop()
go d.Start()

// produce some messages
go func() {
  for i := 0; i < 100; i++ {
    m := fmt.Sprintf("message #%d", i)
    d.Q <- m
  }
}()

// consume batches
for {
  select {
  case batch := <-d.Batch:
    for _, b := range batch {
      s := b.(string)
      // do whatever.
      log.Print(s)
    }
  }
}
```

## Contribute
Improvements, fixes, and feedback are welcome.

## Legal
MIT license.
