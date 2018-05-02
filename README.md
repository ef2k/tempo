`tempo`
=======

Batch queued items by time intervals.

## Features

- **Always available item collection** <br> Enqueue items as they arrive, even during simultaneous processing.

- **Process on a time interval** <br> Items are batched according to the time interval they arrived in.

- **Plain old Go Channels**<br> Implementation is thread safe and backed by inherent channel behavior.

## Install
```sh
go get -u github.com/ef2k/tempo
```

## Example

Collect a stream of messages and process them as a batch every 10 seconds.

```go
package main

import (
  "github.com/ef2k/tempo"
)

type msg string

func main() {

  q := tempo.Q{
    Interval: time.Duration(10) * time.Second,
    MaxItems: 1000,
  }

  // a handler called at the end of each interval
  q.Dispatch(func(ch tempo.C) {
    for item := range ch {
      m := item.(msg)
      log.Print(m)
    }
  })

  // produce some items
  go func() {
    for i := 0; i < 50; i++ {
      m := msg{fmt.Sprintf("producer2/ message #%d", i)}
      q.Enqueue(m)
      time.Sleep(time.Duration(100) * time.Millisecond)
    }
  }()
  go func() {
    for i := 0; i < 50; i++ {
      m := msg{fmt.Sprintf("producer2/ message #%d", i)}
      q.Enqueue(m)
      time.Sleep(time.Duration(100) * time.Millisecond)
    }
  }()

}
```

## Contribute
Improvements, fixes, and feedback are welcome.

## Legal
MIT license.
