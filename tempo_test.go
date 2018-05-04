package tempo

import (
	"fmt"
	"testing"
	"time"
)

type event struct {
	ts   time.Time
	data string
}

func printf(s string, a ...interface{}) {
	if testing.Verbose() {
		fmt.Printf(s, a...)
	}
}

func produce(d *Dispatcher, numProducers, numItems int) {
	count := 0
	for i := 0; i < numProducers; i++ {
		for j := 0; j < numItems; j++ {
			count++
			d.Q <- &event{
				ts:   time.Now(),
				data: fmt.Sprintf("\tProducer #%d, Item #%d", i, j),
			}
		}
	}
}

func TestTempo(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Duration(10) * time.Second,
		MaxBatchItems: 50,
	})

	go d.Start()

	t.Run("should not drop items when exceeding the batch limit", func(t *testing.T) {
		printf("=== TEST The amount of items dispatched should equal the number of items received.\n")
		numItems := 100
		numProducers := 100
		totalItems := numItems * numProducers

		printf("=== INFO Queuing %d items with %d producers.\n", totalItems, numProducers)
		go produce(d, numProducers, numItems)

		breakout := make(chan bool)
		time.AfterFunc(time.Duration(20)*time.Second, func() {
			breakout <- true
		})

		printf("=== INFO Receiving dispatched batches:\n")
		itemCount := 0
	L:
		for {
			select {
			case batch := <-d.Batch:
				batchLen := len(batch)
				printf("%d\t", batchLen)
				itemCount += batchLen
			case <-breakout:
				d.Stop()
				break L
			}
		}
		printf("\n=== INFO Received %d items.\n", itemCount)
		if totalItems != d.DispatchedCount && totalItems != itemCount {
			t.Errorf("Total number of queued items didn't equal the dispatched amount.")
		}
	})
}
