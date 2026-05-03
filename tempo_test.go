package tempo

import (
	"bytes"
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

func payload(s string) []byte {
	return []byte(s)
}

func newDispatcher(t *testing.T, c *Config) *Dispatcher {
	t.Helper()

	d, err := NewDispatcher(c)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	return d
}

func enqueueWithin(t *testing.T, q chan []byte, v []byte, timeout time.Duration) {
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

func receiveBatchWithin(t *testing.T, batches <-chan [][]byte, timeout time.Duration) [][]byte {
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

func produce(q chan []byte, numItems, numGoroutines int, out chan []string) {
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
					q <- []byte(m)
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

func batchStrings(batch [][]byte) []string {
	out := make([]string, len(batch))
	for i, item := range batch {
		out[i] = string(item)
	}
	return out
}

func totalBatchBytes(batch [][]byte) int64 {
	var total int64
	for _, item := range batch {
		total += int64(len(item))
	}
	return total
}

func TestNewDispatcherRejectsInvalidConfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		if _, err := NewDispatcher(nil); err == nil {
			t.Fatal("expected nil config to be rejected")
		}
	})

	t.Run("zero interval", func(t *testing.T) {
		if _, err := NewDispatcher(&Config{Interval: 0, MaxBatchBytes: 10, MaxPendingBytes: 20}); err == nil {
			t.Fatal("expected zero interval to be rejected")
		}
	})

	t.Run("negative interval", func(t *testing.T) {
		if _, err := NewDispatcher(&Config{Interval: -1 * time.Second, MaxBatchBytes: 10, MaxPendingBytes: 20}); err == nil {
			t.Fatal("expected negative interval to be rejected")
		}
	})

	t.Run("zero max batch bytes", func(t *testing.T) {
		if _, err := NewDispatcher(&Config{Interval: time.Second, MaxBatchBytes: 0, MaxPendingBytes: 20}); err == nil {
			t.Fatal("expected zero max batch bytes to be rejected")
		}
	})

	t.Run("negative max batch bytes", func(t *testing.T) {
		if _, err := NewDispatcher(&Config{Interval: time.Second, MaxBatchBytes: -1, MaxPendingBytes: 20}); err == nil {
			t.Fatal("expected negative max batch bytes to be rejected")
		}
	})

	t.Run("zero max pending bytes", func(t *testing.T) {
		if _, err := NewDispatcher(&Config{Interval: time.Second, MaxBatchBytes: 10, MaxPendingBytes: 0}); err == nil {
			t.Fatal("expected zero max pending bytes to be rejected")
		}
	})
}

func TestNewDispatcherAcceptsValidConfig(t *testing.T) {
	d, err := NewDispatcher(&Config{
		Interval:        time.Second,
		MaxBatchBytes:   10,
		MaxPendingBytes: 100,
	})
	if err != nil {
		t.Fatalf("expected valid config to succeed, got %v", err)
	}
	if d == nil {
		t.Fatal("expected a dispatcher instance for valid config")
	}
}

func TestDispatchOrder(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Second,
		MaxBatchBytes:   16 * KiB,
		MaxPendingBytes: 2 * MiB,
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
	time.AfterFunc(3*time.Second, func() {
		breakout <- true
	})
L:
	for {
		select {
		case batch := <-d.Batch:
			dispatched = append(dispatched, batchStrings(batch)...)
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

func TestFlushesImmediatelyWhenBatchBytesAreFull(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   int64(len("first") + len("second")),
		MaxPendingBytes: 128,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)
	enqueueWithin(t, d.Q, payload("second"), 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 200*time.Millisecond)
	if got := batchStrings(batch); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("expected flushed batch to contain first, second; got %#v", got)
	}
}

func TestFlushesPendingItemsWhenIntervalElapses(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected interval flush to emit [first], got %#v", batchStrings(batch))
	}
}

func TestPreservesSequentialOrderWithinBatch(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)
	enqueueWithin(t, d.Q, payload("second"), 100*time.Millisecond)
	enqueueWithin(t, d.Q, payload("third"), 100*time.Millisecond)

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if got := batchStrings(batch); len(got) != 3 || got[0] != "first" || got[1] != "second" || got[2] != "third" {
		t.Fatalf("expected batch order [first second third], got %#v", got)
	}
}

func TestBlockedBatchConsumerTrapsDispatcher(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1,
		MaxPendingBytes: 8,
	})
	go d.Start()

	enqueueWithin(t, d.Q, payload("a"), 100*time.Millisecond)
	enqueueWithin(t, d.Q, payload("b"), 100*time.Millisecond)

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

func TestStopReturnsPromptlyWithoutDrainingBufferedItems(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)
	stopWithin(t, d, 200*time.Millisecond)
}

func TestStopDropsBufferedItems(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)
	stopWithin(t, d, 200*time.Millisecond)

	select {
	case batch := <-d.Batch:
		t.Fatalf("expected Stop to drop buffered items, got flushed batch %#v", batchStrings(batch))
	case <-time.After(150 * time.Millisecond):
	}
}

func TestShutdownFlushesBufferedItemsBeforeReturning(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	enqueueWithin(t, d.Q, payload("first"), 100*time.Millisecond)

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
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected Shutdown to flush [first], got %#v", batchStrings(batch))
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

func TestShutdownReturnsContextErrorWhenDrainCannotComplete(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1,
		MaxPendingBytes: 8,
	})
	go d.Start()

	enqueueWithin(t, d.Q, payload("a"), 100*time.Millisecond)

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

func TestEnqueueAcceptsItemsWhileRunning(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("first")); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected Enqueue path to emit [first], got %#v", batchStrings(batch))
	}
}

func TestEnqueueCopiesPayload(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	original := []byte("first")
	if err := d.Enqueue(original); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}
	copy(original, []byte("mutat"))

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if got := string(batch[0]); got != "first" {
		t.Fatalf("expected queued payload copy to preserve original bytes, got %q", got)
	}
}

func TestEnqueueReturnsErrorAfterStop(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	stopWithin(t, d, 200*time.Millisecond)

	if err := d.Enqueue(payload("first")); err == nil {
		t.Fatal("expected Enqueue to reject new work after Stop")
	}
}

func TestEnqueueReturnsErrorAfterShutdownBegins(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	if err := d.Enqueue(payload("first")); err != nil {
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

	if err := d.Enqueue(payload("second")); err == nil {
		t.Fatal("expected Enqueue to reject new work after Shutdown begins")
	}

	batch := receiveBatchWithin(t, d.Batch, 500*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected Shutdown to drain only the owned item, got %#v", batchStrings(batch))
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

func TestEnqueueReturnsQueueFullAtPendingLimit(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1,
		MaxPendingBytes: 2,
	})
	go d.Start()

	if err := d.Enqueue(payload("a")); err != nil {
		t.Fatalf("expected first Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("b")); err != nil {
		t.Fatalf("expected second Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("c")); err != ErrQueueFull {
		t.Fatalf("expected third Enqueue to fail with ErrQueueFull, got %v", err)
	}

	go func() {
		<-d.Batch
		<-d.Batch
	}()

	if err := shutdownWithin(t, d, time.Second); err != nil {
		t.Fatalf("expected shutdown after draining limited backlog to succeed, got %v", err)
	}
}

func TestEnqueueSucceedsAgainAfterPendingLimitDrains(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1,
		MaxPendingBytes: 2,
	})
	go d.Start()
	t.Cleanup(func() {
		go func() {
			for {
				select {
				case <-d.Batch:
				case <-time.After(10 * time.Millisecond):
					return
				}
			}
		}()
		_ = shutdownWithin(t, d, time.Second)
	})

	if err := d.Enqueue(payload("a")); err != nil {
		t.Fatalf("expected first Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("b")); err != nil {
		t.Fatalf("expected second Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("c")); err != ErrQueueFull {
		t.Fatalf("expected third Enqueue to fail with ErrQueueFull, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batch, 200*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("a")) {
		t.Fatalf("expected first drained batch to contain [a], got %#v", batchStrings(batch))
	}

	if err := d.Enqueue(payload("c")); err != nil {
		t.Fatalf("expected Enqueue to succeed once pending ownership dropped, got %v", err)
	}
}

func TestEnqueueAllowsPayloadLargerThanBatchLimitAndRejectsPendingOverflow(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   4,
		MaxPendingBytes: 8,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("12345")); err != nil {
		t.Fatalf("expected oversized payload to be accepted when it fits within pending limit, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := batchStrings(batch); len(got) != 1 || got[0] != "12345" {
		t.Fatalf("expected oversized payload to flush alone, got %#v", got)
	}
	if got := totalBatchBytes(batch); got != int64(len("12345")) {
		t.Fatalf("expected oversized payload batch to preserve full payload size, got %d", got)
	}

	d2 := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   8,
		MaxPendingBytes: 4,
	})
	go d2.Start()
	t.Cleanup(func() {
		stopWithin(t, d2, 2*time.Second)
	})

	if err := d2.Enqueue(payload("12345")); err != ErrPayloadTooLarge {
		t.Fatalf("expected oversized payload to be rejected by pending limit, got %v", err)
	}
}

func TestFlushesBeforeAppendingPayloadThatWouldOverflowBatch(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   5,
		MaxPendingBytes: 32,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("abc")); err != nil {
		t.Fatalf("expected first enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("de")); err != nil {
		t.Fatalf("expected second enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("fg")); err != nil {
		t.Fatalf("expected third enqueue to succeed, got %v", err)
	}

	first := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := batchStrings(first); len(got) != 2 || got[0] != "abc" || got[1] != "de" {
		t.Fatalf("expected first batch [abc de], got %#v", got)
	}

	second := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := batchStrings(second); len(got) != 1 || got[0] != "fg" {
		t.Fatalf("expected second batch [fg], got %#v", got)
	}
}

func TestBatchByteLimitBoundsEmittedBatchSize(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        100 * time.Millisecond,
		MaxBatchBytes:   4,
		MaxPendingBytes: 32,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	for _, item := range [][]byte{payload("aa"), payload("bb"), payload("cc")} {
		if err := d.Enqueue(item); err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	first := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := totalBatchBytes(first); got > 4 {
		t.Fatalf("expected first batch to stay within byte limit, got %d", got)
	}

	second := receiveBatchWithin(t, d.Batches(), 300*time.Millisecond)
	if got := totalBatchBytes(second); got > 4 {
		t.Fatalf("expected second batch to stay within byte limit, got %d", got)
	}
}

func TestSinglePayloadMayExceedBatchByteLimitWhenItFitsPendingLimit(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   4,
		MaxPendingBytes: 32,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("12345")); err != nil {
		t.Fatalf("expected oversized single payload to be accepted, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := totalBatchBytes(batch); got != int64(len("12345")) {
		t.Fatalf("expected emitted batch to contain full oversized payload, got %d", got)
	}
	if got := batchStrings(batch); len(got) != 1 || got[0] != "12345" {
		t.Fatalf("expected oversized payload to flush as its own batch, got %#v", got)
	}
}

func TestBatchesExposesReadOnlyOutputStream(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        50 * time.Millisecond,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("first")); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 500*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected Batches view to emit [first], got %#v", batchStrings(batch))
	}
}

func TestMethodBasedUsagePreservesBatchingBehavior(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   int64(len("first") + len("second")),
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()
	t.Cleanup(func() {
		stopWithin(t, d, 2*time.Second)
	})

	if err := d.Enqueue(payload("first")); err != nil {
		t.Fatalf("expected first Enqueue to succeed, got %v", err)
	}
	if err := d.Enqueue(payload("second")); err != nil {
		t.Fatalf("expected second Enqueue to succeed, got %v", err)
	}

	batch := receiveBatchWithin(t, d.Batches(), 200*time.Millisecond)
	if got := batchStrings(batch); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("expected flushed batch to contain first, second; got %#v", got)
	}
}

func TestStopIsSafeToCallMoreThanOnce(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	stopWithin(t, d, 200*time.Millisecond)
	stopWithin(t, d, 200*time.Millisecond)
}

func TestShutdownIsSafeToCallMoreThanOnce(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	if err := d.Enqueue(payload("first")); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- d.Shutdown(ctx)
	}()

	batch := receiveBatchWithin(t, d.Batches(), 500*time.Millisecond)
	if len(batch) != 1 || !bytes.Equal(batch[0], payload("first")) {
		t.Fatalf("expected Shutdown to drain [first], got %#v", batchStrings(batch))
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected first Shutdown to succeed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected first Shutdown to complete")
	}

	if err := shutdownWithin(t, d, 200*time.Millisecond); err != nil {
		t.Fatalf("expected second Shutdown to succeed, got %v", err)
	}
}

func TestStopAfterShutdownBeginsDoesNotHang(t *testing.T) {
	d := newDispatcher(t, &Config{
		Interval:        time.Hour,
		MaxBatchBytes:   1 * KiB,
		MaxPendingBytes: 8 * KiB,
	})
	go d.Start()

	if err := d.Enqueue(payload("first")); err != nil {
		t.Fatalf("expected Enqueue to accept items while running, got %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- d.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected Shutdown to still be in progress, returned early with %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	stopWithin(t, d, 200*time.Millisecond)

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Shutdown to stop waiting once Stop wins")
		}
	case <-time.After(time.Second):
		t.Fatal("expected Stop to break the in-progress Shutdown")
	}
}
