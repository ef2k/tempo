package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ef2k/tempo"
)

type event struct {
	ts   time.Time
	data string
}

func generateEvents(d *tempo.Dispatcher) {
	sets := 100
	items := 200

	log.Printf("x Should produce %d items.", sets*items)
	count := 0
	for i := 0; i < sets; i++ {
		for j := 0; j < items; j++ {
			count++
			d.Q <- &event{
				ts:   time.Now(),
				data: fmt.Sprintf("Producer #%d, Item #%d", i, j),
			}
		}
	}
	log.Printf("x Produced %d items.", count)
}

func main() {
	d := tempo.NewDispatcher(&tempo.Config{
		Interval:      time.Duration(10) * time.Second,
		MaxBatchItems: 100,
	})
	defer d.Stop()
	go d.Start()

	// It's important to run this in a goroutine. Otherwise, you'll fill up the
	// channels before getting to read from them, causing a deadlock.
	go generateEvents(d)

	for {
		select {
		case batch := <-d.BatchCh:
			log.Printf("+ Got a batch of %d items.", len(batch))
			log.Printf("+ Dispatched so far: %d", d.DispatchedCount)
			// for _, b := range batch {
			// 	evt := b.(*event)
			// 	// fmt.Printf("\t[event]: %s\n", evt.data)
			// }
		}
	}
}
