package tempo

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func printf(s string, a ...interface{}) {
	if testing.Verbose() {
		fmt.Printf(s, a...)
	}
}

func enqueueWithin(t *testing.T, q chan item, v item, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		q <- v
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("enqueue timed out after %s", timeout)
	}
}

func receiveBatchWithin(t *testing.T, batches chan []item, timeout time.Duration) []item {
	t.Helper()

	select {
	case batch := <-batches:
		return batch
	case <-time.After(timeout):
		t.Fatalf("batch was not received within %s", timeout)
		return nil
	}
}

func stopWithin(t *testing.T, d *Dispatcher, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("stop timed out after %s", timeout)
	}
}

func produce(q chan item, numItems, numGoroutines int, out chan []string) {
	printf("=== Producing %d items.\n", numItems*numGoroutines)
	done := make(chan bool, 1)
	msgs := make(chan string)
	var wg sync.WaitGroup
	go func() {
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(routineIdx int) {
				for j := 0; j < numItems; j++ {
					m := fmt.Sprintf("producer#%d, item#%d", routineIdx, j)
					q <- m
					msgs <- m
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		done <- true
	}()
	var coll []string
L:
	for {
		select {
		case m := <-msgs:
			coll = append(coll, m)
		case <-done:
			break L
		}
	}
	out <- coll
}

// Test: Under concurrent production, accept a full wave of items and emit them
// back out without dropping or inventing messages.
//
// Here we start many producers at once and let them push enough items to cross
// the batch limit repeatedly. The observable signal is the final set of
// dispatched items, but the broader requirement is that rollover under load
// preserves the complete produced set.
func TestDispatchOrder(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Duration(1) * time.Second,
		MaxBatchItems: 500,
	})
	defer d.Stop()
	go d.Start()

	var (
		numItems      = 10
		numGoroutines = 100
	)

	produced := make(chan []string, 1)
	go produce(d.Q, numItems, numGoroutines, produced)

	var dispatched []string
	breakout := make(chan bool)
	time.AfterFunc(time.Duration(3)*time.Second, func() {
		breakout <- true
	})
L:
	for {
		select {
		case batch := <-d.Batch:
			for _, b := range batch {
				m := b.(string)
				dispatched = append(dispatched, m)
			}
		case <-breakout:
			break L
		}
	}
	msgs := <-produced

	t.Run("no lost items", func(t *testing.T) {
		if len(msgs) != len(dispatched) {
			t.Error("dispatched messages are missing")
		}
	})

	t.Run("confirm dispatched items", func(t *testing.T) {
		msgExists := make(map[string]bool)
		for i := 0; i < len(msgs); i++ {
			msgExists[msgs[i]] = false
		}
		for i := 0; i < len(dispatched); i++ {
			if _, ok := msgExists[dispatched[i]]; !ok {
				t.Errorf("item was not dispatched: %s", dispatched[i])
			}
		}
	})
}

// Test: Flush immediately when the batch reaches its configured size.
//
// Here we configure a small batch limit and enqueue exactly enough items to hit
// that limit without waiting for the interval. The observable signal is that a
// batch is emitted right away, but the broader requirement is that rollover by
// capacity does not depend on the timer to make progress.
func TestFlushesImmediatelyWhenBatchIsFull(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 2,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)
	enqueueWithin(t, d.Q, "second", 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 200*time.Millisecond)
	if len(batch) != 2 {
		t.Fatalf("expected 2 items in flushed batch, got %d", len(batch))
	}
	if batch[0] != "first" || batch[1] != "second" {
		t.Fatalf("expected flushed batch to contain first, second; got %#v", batch)
	}
}

// Test: Flush pending items when the interval elapses, even if the batch is not
// full.
//
// Here we enqueue a single item into a dispatcher with a large batch limit so
// that time, not capacity, is what forces the flush. The observable signal is
// that the item is eventually emitted, but the broader requirement is that
// partial batches do not remain stranded while waiting for more traffic.
func TestFlushesPendingItemsWhenIntervalElapses(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      50 * time.Millisecond,
		MaxBatchItems: 10,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || batch[0] != "first" {
		t.Fatalf("expected interval flush to emit [first], got %#v", batch)
	}
}

// Test: Preserve enqueue order within a batch for sequential input.
//
// Here we enqueue a small ordered sequence into a dispatcher with a batch limit
// large enough to keep all of those items together until the interval fires.
// The observable signal is the item order in the emitted batch, but the broader
// requirement is that Tempo must not reorder a single producer's stream while
// assembling a batch.
func TestPreservesSequentialOrderWithinBatch(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      50 * time.Millisecond,
		MaxBatchItems: 10,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)
	enqueueWithin(t, d.Q, "second", 100*time.Millisecond)
	enqueueWithin(t, d.Q, "third", 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 3 {
		t.Fatalf("expected 3 items in batch, got %d", len(batch))
	}
	if batch[0] != "first" || batch[1] != "second" || batch[2] != "third" {
		t.Fatalf("expected batch order [first second third], got %#v", batch)
	}
}

// Test: When under high-frequency, keep coordinating intake, rollover, and
// shutdown even when batch delivery is under pressure.
//
// Here we force the dispatcher into the "batch full" path while no consumer is
// reading d.Batch. Publishing that full batch must not block the dispatch loop
// so completely that the dispatcher can no longer coordinate its own work.
//
// Stop() is the visible signal in this test, but the issue being covered is
// broader. The dispatcher must preserve internal liveness even when batch
// delivery is momentarily blocked.
func TestBlockedBatchConsumerTrapsDispatcher(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 1,
	})
	go d.Start()

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)
	enqueueWithin(t, d.Q, "second", 100*time.Millisecond)

	// Give the dispatcher a moment to hit the "batch full" path and block while
	// trying to publish the first batch to the unread Batch channel.
	time.Sleep(50 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		d.Stop()
		close(stopDone)
	}()

	blocked := false
	select {
	case <-stopDone:
	case <-time.After(150 * time.Millisecond):
		blocked = true
	}

	go func() {
		<-d.Batch
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not stop after the blocked batch was drained")
	}

	if blocked {
		t.Fatal("stop blocked because the dispatcher was trapped trying to dispatch a full batch")
	}
}
