`tempo`
=======

A dispatched batch queue that processes items at time intervals or as soon as a batching limit is met.

## Features 

- **Non-blocking enqueue** <br> Queue up incoming items without blocking processing.

- **Processing by periodic time intervals** <br> Items are batched for processing by the interval of time they arrive in.

- **Processing as soon as a batch limit is met**<br> If the batch limit is reached before the time interval expires, items are processed immediately. 

- **Plain old Go channels** <br> Implementation leverages the normal syncing behavior of channels. It's free of mutexes and other bookkeeping techniques.

## Install
```sh
$ dep ensure -add github.com/ef2k/tempo
```

## Example

Collect a constant stream of messages and process them as every 10 seconds or as soon as we reach a batching limit.

```go
package main

import (
  "github.com/ef2k/tempo"
)

type msg string

func main() {

  // init a dispatcher
  d := tempo.NewDispatcher(&tempo.Config{
    Interval: time.Duration(10) * time.Second,
    MaxBatchItems: 100,
  })

  // start it up in its own goroutine.
  go d.Start()


  // produce lots of messages...

  go func() {
    for i := 0; i < 50; i++ {
      m := msg{fmt.Sprintf("producer1/ message #%d", i)}
      d.Q <- m
      time.Sleep(time.Duration(100) * time.Millisecond)
    }
  }()
  go func() {
    for i := 0; i < 50; i++ {
      m := msg{fmt.Sprintf("producer2/ message #%d", i)}
      d.Q <- m
      time.Sleep(time.Duration(100) * time.Millisecond)
    }
  }()

  // get the batch and print them out.
  for {
    select {
    case b := <-d.BatchCh:
      log.Println(b)
    }
  }
}
```

## Contribute
Improvements, fixes, and feedback are welcome.

## Legal
MIT license.
