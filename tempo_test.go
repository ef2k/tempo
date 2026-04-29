package tempo

import (
	"context"
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

func receiveBatchWithin(t *testing.T, batches <-chan []item, timeout time.Duration) []item {
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

func shutdownWithin(t *testing.T, d *Dispatcher, timeout time.Duration) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- d.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout + 100*time.Millisecond):
		t.Fatalf("shutdown timed out after %s", timeout)
		return nil
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

// Test: Stop returns promptly without waiting for buffered items to drain.
//
// Here we enqueue an item into a partially filled batch and then call Stop
// before either the interval or batch size would force a flush. The observable
// signal is that Stop returns quickly, but the broader requirement is that the
// immediate-stop path must not depend on draining buffered work first.
func TestStopReturnsPromptlyWithoutDrainingBufferedItems(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 10,
	})
	go d.Start()

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)
	stopWithin(t, d, 200*time.Millisecond)
}

// Test: Stop abandons buffered items instead of flushing them.
//
// Here we enqueue an item and stop the dispatcher before any normal flush
// trigger occurs. The observable signal is that no batch is emitted after Stop
// returns, but the broader requirement is that immediate stop and graceful
// drain are different lifecycle operations.
func TestStopDropsBufferedItems(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 10,
	})
	go d.Start()

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)
	stopWithin(t, d, 200*time.Millisecond)

	select {
	case batch := <-d.Batch:
		t.Fatalf("expected Stop to drop buffered items, got flushed batch %#v", batch)
	case <-time.After(150 * time.Millisecond):
	}
}

// Test: Shutdown flushes buffered items before returning.
//
// Here we enqueue an item into a partial batch and call Shutdown before any
// timer-based flush would occur. The observable signal is that Shutdown returns
// only after the final buffered item is emitted, but the broader requirement is
// that graceful drain preserves in-flight work.
func TestShutdownFlushesBufferedItemsBeforeReturning(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 10,
	})
	go d.Start()

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- d.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected Shutdown to wait for buffered delivery, returned early with %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || batch[0] != "first" {
		t.Fatalf("expected Shutdown to flush [first], got %#v", batch)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected Shutdown to succeed after flushing buffered items, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected Shutdown to return after flushing buffered items")
	}
}

// Test: Shutdown respects context cancellation when graceful drain cannot
// complete.
//
// Here we enqueue enough items to form a full batch and then call Shutdown
// without any batch consumer to receive it. The observable signal is that
// Shutdown returns the context error, but the broader requirement is that
// graceful drain must be externally bounded when delivery cannot finish.
func TestShutdownReturnsContextErrorWhenDrainCannotComplete(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 1,
	})
	go d.Start()

	enqueueWithin(t, d.Q, "first", 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := d.Shutdown(ctx)
	if err == nil {
		t.Fatal("expected Shutdown to return a context error when drain cannot complete")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

// Test: Enqueue accepts items while the dispatcher is running.
//
// Interact with tempo through an API layer, encapsulating its internals. this
// way the caller has a way to know if the queue is available when adding items
// or if its not accepting items because its in the middle of a shutdown.
func TestEnqueueAcceptsItemsWhileRunning(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      50 * time.Millisecond,
		MaxBatchItems: 10,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue("first"); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || batch[0] != "first" {
		t.Fatalf("expected Enqueue path to emit [first], got %#v", batch)
	}
}

// Test: Enqueue returns an error after Stop begins.
//
// Here we stop the dispatcher and then attempt to enqueue through the method
// API. Tempo should signal an enqueue error, but the broader requirement
// is that Tempo must reject new work explicitly once immediate shutdown starts
// instead of leaving callers to block on internal channels.
func TestEnqueueReturnsErrorAfterStop(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 10,
	})
	go d.Start()

	stopWithin(t, d, 200*time.Millisecond)

	if err := d.Enqueue("first"); err == nil {
		t.Fatal("expected Enqueue to reject new work after Stop")
	}
}

// Test: Enqueue returns an error after graceful shutdown begins.
//
// Here we start Shutdown and then attempt to enqueue through the method API
// before the dispatcher exits. Tempo should signal an enqueue error, but
// the broader requirement is that graceful drain must stop accepting new work
// while it finishes what's already in the buffer.
func TestEnqueueReturnsErrorAfterShutdownBegins(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 10,
	})
	go d.Start()

	if err := d.Enqueue("first"); err != nil {
		t.Fatalf("expected initial enqueue to succeed, got %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- d.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected Shutdown to wait for buffered delivery, returned early with %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := d.Enqueue("second"); err == nil {
		t.Fatal("expected Enqueue to reject new work after Shutdown begins")
	}

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || batch[0] != "first" {
		t.Fatalf("expected Shutdown to drain only the owned item, got %#v", batch)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected Shutdown to complete successfully, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected Shutdown to complete after draining owned work")
	}
}

// Test: Batches exposes Tempo's output stream as a read-only API surface.
//
// Here we consume batches through the method API instead of reading the
// exported channel directly. Tempo should signal that the emitted batch is
// still available to callers, but the broader requirement is that Tempo can
// expose consumption without forcing callers to depend on writing to internals.
func TestBatchesExposesReadOnlyOutputStream(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      50 * time.Millisecond,
		MaxBatchItems: 10,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue("first"); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 500*time.Millisecond)
	if len(batch) != 1 || batch[0] != "first" {
		t.Fatalf("expected Batches view to emit [first], got %#v", batch)
	}
}

// Test: Method-based usage preserves Tempo's batching behavior.
//
// Here we drive both submission and consumption through Enqueue and Batches
// rather than the directly to internals.
func TestMethodBasedUsagePreservesBatchingBehavior(t *testing.T) {
	d := NewDispatcher(&Config{
		Interval:      time.Hour,
		MaxBatchItems: 2,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue("first"); err != nil {
		t.Fatalf("expected first Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue("second"); err != nil {
		t.Fatalf("expected second Enqueue to succeed, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if len(batch) != 2 {
		t.Fatalf("expected 2 items in flushed batch, got %d", len(batch))
	}
	if batch[0] != "first" || batch[1] != "second" {
		t.Fatalf("expected flushed batch to contain first, second; got %#v", batch)
	}
}
