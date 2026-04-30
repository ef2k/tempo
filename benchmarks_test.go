package tempo

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type benchEvent struct {
	id   int
	data string
}

type benchLargeEvent struct {
	id      int
	payload [256]byte
}

type benchDrainOptions struct {
	consumerDelay time.Duration
}

func newBenchDispatcher(b *testing.B, c *Config, opts benchDrainOptions) (*Dispatcher, chan struct{}, *atomic.Int64, *atomic.Int64) {
	b.Helper()

	d, err := NewDispatcher(c)
	if err != nil {
		b.Fatalf("new dispatcher: %v", err)
	}

	stopDrain := make(chan struct{})
	delivered := &atomic.Int64{}
	batches := &atomic.Int64{}

	go func() {
		for {
			select {
			case batch := <-d.Batches():
				if opts.consumerDelay > 0 {
					time.Sleep(opts.consumerDelay)
				}
				delivered.Add(int64(len(batch)))
				batches.Add(1)
			case <-stopDrain:
				return
			}
		}
	}()

	go d.Start()

	b.Cleanup(func() {
		d.Stop()
		close(stopDrain)
	})

	return d, stopDrain, delivered, batches
}

func waitForDelivered(b *testing.B, delivered *atomic.Int64, expected int64, timeout time.Duration) {
	b.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if delivered.Load() >= expected {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}

	b.Fatalf("timed out waiting for delivered items: got %d want %d", delivered.Load(), expected)
}

func shutdownBenchDispatcher(b *testing.B, d *Dispatcher, timeout time.Duration) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := d.Shutdown(ctx); err != nil {
		b.Fatalf("shutdown: %v", err)
	}
}

func reportBenchMetrics(b *testing.B, delivered, batches *atomic.Int64) {
	b.Helper()

	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	b.ReportMetric(float64(delivered.Load()), "items_delivered")
	b.ReportMetric(float64(batches.Load()), "batches_delivered")
	b.ReportMetric(float64(delivered.Load())/elapsed, "items/sec")
	b.ReportMetric(float64(batches.Load())/elapsed, "batches/sec")
}

func slowConsumerDrainTimeout(n int, maxBatchItems int, delay time.Duration) time.Duration {
	expectedBatches := n / maxBatchItems
	if n%maxBatchItems != 0 {
		expectedBatches++
	}
	timeout := time.Duration(expectedBatches)*delay + 2*time.Second
	if timeout < 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

// BenchmarkEnqueueSingleProducer measures the basic single-producer size-driven
// path without contention.
func BenchmarkEnqueueSingleProducer(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 256,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEvent{id: i, data: "event"}); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkSustainedParallelFlush256 measures steady parallel producer
// throughput when batching is mostly driven by max batch size.
func BenchmarkSustainedParallelFlush256(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 256,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkBurstParallelFlush64 measures bursty parallel producers hitting
// smaller size-based flushes more often.
func BenchmarkBurstParallelFlush64(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 64,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.SetParallelism(32)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkBatchDeliverySmallBatch measures throughput when Tempo emits many
// small batches instead of larger grouped flushes.
func BenchmarkBatchDeliverySmallBatch(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 8,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEvent{id: i, data: "event"}); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkParallelLargePayload measures how larger item size affects parallel
// producer throughput.
func BenchmarkParallelLargePayload(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 256,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchLargeEvent{id: id}); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 3*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 3*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkSlowConsumerBackpressure measures producer throughput when batch
// delivery is intentionally slowed down.
func BenchmarkSlowConsumerBackpressure(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      time.Hour,
		MaxBatchItems: 64,
	}, benchDrainOptions{
		consumerDelay: 50 * time.Microsecond,
	})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 5*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 5*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkIntervalDrivenBatching measures the timer-driven path where flushes
// happen on the interval instead of max batch size.
func BenchmarkIntervalDrivenBatching(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &Config{
		Interval:      200 * time.Microsecond,
		MaxBatchItems: b.N + 1,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEvent{id: i, data: "event"}); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
		time.Sleep(300 * time.Microsecond)
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

// BenchmarkBatchSizeSweep compares throughput as max batch size changes under
// the same sustained parallel producer load.
func BenchmarkBatchSizeSweep(b *testing.B) {
	for _, size := range []int{8, 32, 128, 512, 1024} {
		b.Run("max_batch_items="+itoa(size), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &Config{
				Interval:      time.Hour,
				MaxBatchItems: size,
			}, benchDrainOptions{})

			var seq atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
						b.Fatalf("enqueue: %v", err)
					}
				}
			})

			b.StopTimer()
			shutdownBenchDispatcher(b, d, 3*time.Second)
			waitForDelivered(b, delivered, int64(b.N), 3*time.Second)
			reportBenchMetrics(b, delivered, batches)
		})
	}
}

// BenchmarkProducerCountSweep compares throughput as producer concurrency
// increases under the same size-driven flush settings.
func BenchmarkProducerCountSweep(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16, 64, 256} {
		b.Run("parallelism="+itoa(parallelism), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &Config{
				Interval:      time.Hour,
				MaxBatchItems: 256,
			}, benchDrainOptions{})

			var seq atomic.Int64

			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
						b.Fatalf("enqueue: %v", err)
					}
				}
			})

			b.StopTimer()
			shutdownBenchDispatcher(b, d, 3*time.Second)
			waitForDelivered(b, delivered, int64(b.N), 3*time.Second)
			reportBenchMetrics(b, delivered, batches)
		})
	}
}

// BenchmarkIntervalSweep compares timer-driven throughput as the flush interval
// changes.
func BenchmarkIntervalSweep(b *testing.B) {
	for _, interval := range []time.Duration{
		100 * time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
	} {
		b.Run("interval="+interval.String(), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &Config{
				Interval:      interval,
				MaxBatchItems: b.N + 1,
			}, benchDrainOptions{})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := d.Enqueue(benchEvent{id: i, data: "event"}); err != nil {
					b.Fatalf("enqueue: %v", err)
				}
				time.Sleep(interval + interval/2)
			}

			b.StopTimer()
			shutdownBenchDispatcher(b, d, 3*time.Second)
			waitForDelivered(b, delivered, int64(b.N), 3*time.Second)
			reportBenchMetrics(b, delivered, batches)
		})
	}
}

// BenchmarkSlowConsumerSweep compares throughput as batch delivery slows down.
func BenchmarkSlowConsumerSweep(b *testing.B) {
	const maxBatchItems = 256

	for _, delay := range []time.Duration{
		10 * time.Microsecond,
		100 * time.Microsecond,
		time.Millisecond,
	} {
		b.Run("consumer_delay="+delay.String(), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &Config{
				Interval:      time.Hour,
				MaxBatchItems: maxBatchItems,
			}, benchDrainOptions{
				consumerDelay: delay,
			})

			var seq atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEvent{id: id, data: "event"}); err != nil {
						b.Fatalf("enqueue: %v", err)
					}
				}
			})

			b.StopTimer()
			timeout := slowConsumerDrainTimeout(b.N, maxBatchItems, delay)
			shutdownBenchDispatcher(b, d, timeout)
			waitForDelivered(b, delivered, int64(b.N), timeout)
			reportBenchMetrics(b, delivered, batches)
		})
	}
}

func itoa(v int) string {
	return strconv.Itoa(v)
}
