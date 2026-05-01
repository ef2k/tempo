package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ef2k/tempo"
)

func main() {
	log.SetFlags(log.Lmicroseconds)

	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:        3 * time.Second,
		MaxBatchBytes:   64 * tempo.KiB,
		MaxPendingBytes: 8 * tempo.MiB,
	})
	if err != nil {
		log.Fatalf("new dispatcher: %v", err)
	}
	// Start the dispatcher loop before producing or consuming batches.
	go d.Start()

	// Make some items
	numGoroutines := 10
	numItems := 100
	expectedItems := numGoroutines * numItems
	log.Printf("  RUN SUMMARY")
	log.Printf("  ---")
	log.Printf("  tempo interval:         %s", d.Interval)
	log.Printf("  tempo max batch bytes:  %d", d.MaxBatchBytes)
	log.Printf("  tempo max pending:      %d", d.MaxPendingBytes)
	log.Printf("  ---")
	log.Printf("  producer items:        %d", expectedItems)
	log.Printf("  producer goroutines:   %d", numGoroutines)
	log.Printf("  producer cadence:      800ms per goroutine")
	log.Printf("  ---")

	go func() {
		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				for j := 0; j < numItems; j++ {
					// Enqueue is the producer-facing API for submitting work.
					if err := d.Enqueue([]byte(fmt.Sprintf("(producer#%d, item#%d)", i, j))); err != nil {
						log.Printf("enqueue failed: %v", err)
						return
					}
					time.Sleep(time.Duration(800) * time.Millisecond)
				}
			}(i)
		}
	}()

	// Consume the items
	dispatched := 0
	for {
		select {
		// Batches exposes the consumer-facing stream as receive-only.
		case batch := <-d.Batches():
			log.Printf("Batch received with %d items.", len(batch))
			dispatched += len(batch)
			for _, b := range batch {
				fmt.Printf("%s\t", string(b))
			}
			fmt.Println("\n===")
			if dispatched == expectedItems {
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
