package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/ef2k/tempo"
)

type event struct {
	TS   time.Time `json:"ts"`
	Data string    `json:"data"`
}

func generateEvents(d *tempo.Dispatcher, sets, items int) {
	log.Printf("x Should produce %d items.", sets*items)
	count := 0
	for i := 0; i < sets; i++ {
		for j := 0; j < items; j++ {
			count++
			payload, err := json.Marshal(event{
				TS:   time.Now(),
				Data: "Producer payload",
			})
			if err != nil {
				log.Printf("marshal failed: %v", err)
				return
			}
			if err := d.Enqueue(payload); err != nil {
				log.Printf("enqueue failed: %v", err)
				return
			}
		}
	}
	log.Printf("x Produced %d items.", count)
}

func main() {
	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:         10 * time.Second,
		MaxBatchBytes:    64 * tempo.KiB,
		MaxBufferedBytes: 8 * tempo.MiB,
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
	log.Printf("  tempo max batch bytes:  %d", d.MaxBatchBytes)
	log.Printf("  tempo max buffered:      %d", d.MaxBufferedBytes)
	log.Printf("  payload mode:           []byte (json)")
	log.Printf("  ---")
	log.Printf("  producer sets:          %d", sets)
	log.Printf("  items per set:          %d", items)
	log.Printf("  items produced:         %d", expectedItems)
	log.Printf("  ---")
	go generateEvents(d, sets, items)

	dispatched := 0
	for {
		select {
		// Batches exposes the consumer-facing stream as receive-only.
		case batch := <-d.Batches():
			log.Printf("+ Got a batch of %d items.", len(batch))
			for _, raw := range batch {
				var e event
				if err := json.Unmarshal(raw, &e); err != nil {
					log.Printf("unmarshal failed: %v", err)
					return
				}
			}
			dispatched += len(batch)
			log.Printf("+ Dispatched so far: %d", dispatched)
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
