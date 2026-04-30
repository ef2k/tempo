package performance

import (
	"context"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tempo "github.com/ef2k/tempo"
)

// These tests sit apart from the main contract suite on purpose.
//
// A stress test pushes Tempo harder than normal to see whether it still makes
// progress under heavy concurrency and pressure.
//
// A soak test keeps Tempo running under ongoing load for longer and watches for
// slower problems like wedges, unexpected memory spikes, or goroutine buildup.

// TestStressHighConcurrencyDelivery proves that Tempo can accept and deliver a
// large wave of concurrently produced items without wedging or losing work.
func TestStressHighConcurrencyDelivery(t *testing.T) {
	if os.Getenv("TEMPO_RUN_STRESS") == "" {
		t.Skip("set TEMPO_RUN_STRESS=1 to run stress tests")
	}

	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:      time.Hour,
		MaxBatchItems: 256,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	go d.Start()

	const (
		numProducers      = 256
		itemsPerProducer  = 2000
		expectedDelivered = numProducers * itemsPerProducer
	)

	var produced sync.WaitGroup
	produced.Add(numProducers)

	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer produced.Done()
			for j := 0; j < itemsPerProducer; j++ {
				if err := d.Enqueue(benchEvent{id: producerID*itemsPerProducer + j, data: "stress"}); err != nil {
					t.Errorf("enqueue failed: %v", err)
					return
				}
			}
		}(i)
	}

	doneProducing := make(chan struct{})
	go func() {
		produced.Wait()
		close(doneProducing)
	}()

	delivered := 0
	deadline := time.After(20 * time.Second)

	for delivered < expectedDelivered {
		select {
		case batch := <-d.Batches():
			delivered += len(batch)
		case <-deadline:
			t.Fatalf("timed out under stress load: delivered %d of %d items", delivered, expectedDelivered)
		}
	}

	select {
	case <-doneProducing:
	case <-time.After(2 * time.Second):
		t.Fatal("producers did not finish after all items were delivered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown under stress load: %v", err)
	}
}

// TestSoakSustainedLoadStaysHealthy proves that Tempo can run under sustained
// load for a while without wedging and without obvious goroutine growth.
func TestSoakSustainedLoadStaysHealthy(t *testing.T) {
	if os.Getenv("TEMPO_RUN_SOAK") == "" {
		t.Skip("set TEMPO_RUN_SOAK=1 to run soak tests")
	}

	runFor := 5 * time.Minute
	if raw := os.Getenv("TEMPO_SOAK_DURATION"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse TEMPO_SOAK_DURATION: %v", err)
		}
		runFor = parsed
	}

	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:      10 * time.Millisecond,
		MaxBatchItems: 128,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	go d.Start()

	const numProducers = 32

	startGoroutines := runtime.NumGoroutine()
	var delivered atomic.Int64

	stopConsumers := make(chan struct{})
	go func() {
		for {
			select {
			case batch := <-d.Batches():
				delivered.Add(int64(len(batch)))
			case <-stopConsumers:
				return
			}
		}
	}()

	var producers sync.WaitGroup
	producers.Add(numProducers)

	stopProducing := make(chan struct{})
	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer producers.Done()
			seq := 0
			for {
				select {
				case <-stopProducing:
					return
				default:
					if err := d.Enqueue(benchEvent{id: producerID<<20 | seq, data: "soak"}); err != nil {
						return
					}
					seq++
				}
			}
		}(i)
	}

	time.Sleep(runFor)
	close(stopProducing)
	producers.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after soak run: %v", err)
	}
	close(stopConsumers)

	if delivered.Load() == 0 {
		t.Fatal("expected soak run to deliver some items")
	}

	endGoroutines := runtime.NumGoroutine()
	if endGoroutines > startGoroutines+16 {
		t.Fatalf("unexpected goroutine growth during soak run: start=%d end=%d", startGoroutines, endGoroutines)
	}
}
