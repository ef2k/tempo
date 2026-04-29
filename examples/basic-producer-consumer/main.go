package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ef2k/tempo"
)

type event struct {
	ts   time.Time
	data string
}

func generateEvents(d *tempo.Dispatcher, sets, items int) {
	log.Printf("x Should produce %d items.", sets*items)
	count := 0
	for i := 0; i < sets; i++ {
		for j := 0; j < items; j++ {
			count++
			if err := d.Enqueue(&event{
				ts:   time.Now(),
				data: fmt.Sprintf("Producer #%d, Item #%d", i, j),
			}); err != nil {
				log.Printf("enqueue failed: %v", err)
				return
			}
		}
	}
	log.Printf("x Produced %d items.", count)
}

func main() {
	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:      time.Duration(10) * time.Second,
		MaxBatchItems: 100,
	})
	if err != nil {
		log.Fatalf("new dispatcher: %v", err)
	}

	// Start the dispatcher loop before producing or consuming batches.
	go d.Start()

	sets := 100
	items := 200
	expectedItems := sets * items
	log.Printf("  RUN SUMMARY")
	log.Printf("  ---")
	log.Printf("  tempo interval:         %s", d.Interval)
	log.Printf("  tempo items per batch:  %d", d.MaxBatchItems)
	log.Printf("  ---")
	log.Printf("  producer sets:          %d", sets)
	log.Printf("  items per set:          %d", items)
	log.Printf("  items produced:         %d", expectedItems)
	log.Printf("  ---")
	go generateEvents(d, sets, items)

	for {
		select {
		// Batches exposes the consumer-facing stream as receive-only.
		case batch := <-d.Batches():
			log.Printf("+ Got a batch of %d items.", len(batch))
			log.Printf("+ Dispatched so far: %d", d.DispatchedCount)
			if d.DispatchedCount == expectedItems {
				// Use Shutdown to drain any Tempo-owned work before exiting.
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := d.Shutdown(ctx); err != nil {
					log.Printf("shutdown failed: %v", err)
				}
				return
			}
		}
	}
}
