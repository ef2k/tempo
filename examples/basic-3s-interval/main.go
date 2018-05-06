package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ef2k/tempo"
)

func main() {
	log.SetFlags(log.Lmicroseconds)

	d := tempo.NewDispatcher(&tempo.Config{
		Interval:      time.Duration(3) * time.Second,
		MaxBatchItems: 100,
	})
	defer d.Stop()
	go d.Start()

	// Make some items
	numGoroutines := 10
	numItems := 100
	go func() {
		for i := 0; i < numGoroutines; i++ {
			go func(i int) {
				for j := 0; j < numItems; j++ {
					d.Q <- fmt.Sprintf("(producer#%d, item#%d)", i, j)
					time.Sleep(time.Duration(800) * time.Millisecond)
				}
			}(i)
		}
	}()

	// Consume the items
	for {
		select {
		case batch := <-d.Batch:
			log.Printf("Batch received with %d items.", len(batch))
			for _, b := range batch {
				m := b.(string)
				fmt.Printf("%s\t", m)
			}
			fmt.Println("\n===")
		}
	}
}
