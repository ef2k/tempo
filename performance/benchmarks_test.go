package performance

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	tempo "github.com/ef2k/tempo"
)

func benchEventPayload(id int) []byte {
	return []byte("event:" + strconv.Itoa(id))
}

func benchLargeEventPayload(id int) []byte {
	payload := make([]byte, 256)
	copy(payload, []byte("large:"+strconv.Itoa(id)))
	return payload
}

func benchPayloadBytes() int64 {
	return int64(len(benchEventPayload(0)))
}

func benchLargePayloadBytes() int64 {
	return int64(len(benchLargeEventPayload(0)))
}

type benchDrainOptions struct {
	consumerDelay time.Duration
}

func newBenchDispatcher(b *testing.B, c *tempo.Config, opts benchDrainOptions) (*tempo.Dispatcher, chan struct{}, *atomic.Int64, *atomic.Int64) {
	b.Helper()

	d, err := tempo.NewDispatcher(c)
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

func shutdownBenchDispatcher(b *testing.B, d *tempo.Dispatcher, timeout time.Duration) {
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

func slowConsumerDrainTimeout(n int, maxBatchBytes, payloadBytes int64, delay time.Duration) time.Duration {
	itemsPerBatch := maxBatchBytes / payloadBytes
	if itemsPerBatch <= 0 {
		itemsPerBatch = 1
	}
	expectedBatches := int64(n) / itemsPerBatch
	if int64(n)%itemsPerBatch != 0 {
		expectedBatches++
	}
	timeout := time.Duration(expectedBatches)*delay + 2*time.Second
	if timeout < 5*time.Second {
		return 5 * time.Second
	}
	return timeout
}

func BenchmarkEnqueueSingleProducer(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   256 * benchPayloadBytes(),
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEventPayload(i)); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkSustainedParallelFlush256(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   256 * benchPayloadBytes(),
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEventPayload(id)); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkBurstParallelFlush64(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   64 * benchPayloadBytes(),
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.SetParallelism(32)
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEventPayload(id)); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkBatchDeliverySmallBatch(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   8 * benchPayloadBytes(),
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEventPayload(i)); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkParallelLargePayload(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   256 * benchLargePayloadBytes(),
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchLargeEventPayload(id)); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 3*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 3*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkSlowConsumerBackpressure(b *testing.B) {
	maxBatchBytes := int64(64) * benchPayloadBytes()
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        time.Hour,
		MaxBatchBytes:   maxBatchBytes,
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{
		consumerDelay: 50 * time.Microsecond,
	})

	var seq atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(seq.Add(1))
			if err := d.Enqueue(benchEventPayload(id)); err != nil {
				b.Fatalf("enqueue: %v", err)
			}
		}
	})

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 5*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 5*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkIntervalDrivenBatching(b *testing.B) {
	d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
		Interval:        200 * time.Microsecond,
		MaxBatchBytes:   1 * tempo.GiB,
		MaxPendingBytes: 1 * tempo.GiB,
	}, benchDrainOptions{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := d.Enqueue(benchEventPayload(i)); err != nil {
			b.Fatalf("enqueue: %v", err)
		}
		time.Sleep(300 * time.Microsecond)
	}

	b.StopTimer()
	shutdownBenchDispatcher(b, d, 2*time.Second)
	waitForDelivered(b, delivered, int64(b.N), 2*time.Second)
	reportBenchMetrics(b, delivered, batches)
}

func BenchmarkBatchSizeSweep(b *testing.B) {
	for _, size := range []int{8, 32, 128, 512, 1024} {
		b.Run("max_batch_bytes="+itoa(size), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
				Interval:        time.Hour,
				MaxBatchBytes:   int64(size) * benchPayloadBytes(),
				MaxPendingBytes: 1 * tempo.GiB,
			}, benchDrainOptions{})

			var seq atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEventPayload(id)); err != nil {
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

func BenchmarkProducerCountSweep(b *testing.B) {
	for _, parallelism := range []int{1, 4, 16, 64, 256} {
		b.Run("parallelism="+itoa(parallelism), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
				Interval:        time.Hour,
				MaxBatchBytes:   256 * benchPayloadBytes(),
				MaxPendingBytes: 1 * tempo.GiB,
			}, benchDrainOptions{})

			var seq atomic.Int64

			b.SetParallelism(parallelism)
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEventPayload(id)); err != nil {
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

func BenchmarkIntervalSweep(b *testing.B) {
	for _, interval := range []time.Duration{
		100 * time.Microsecond,
		time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
	} {
		b.Run("interval="+interval.String(), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
				Interval:        interval,
				MaxBatchBytes:   1 * tempo.GiB,
				MaxPendingBytes: 1 * tempo.GiB,
			}, benchDrainOptions{})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := d.Enqueue(benchEventPayload(i)); err != nil {
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

func BenchmarkSlowConsumerSweep(b *testing.B) {
	const batchItems = 256
	maxBatchBytes := int64(batchItems) * benchPayloadBytes()

	for _, delay := range []time.Duration{
		10 * time.Microsecond,
		100 * time.Microsecond,
		time.Millisecond,
	} {
		b.Run("consumer_delay="+delay.String(), func(b *testing.B) {
			d, _, delivered, batches := newBenchDispatcher(b, &tempo.Config{
				Interval:        time.Hour,
				MaxBatchBytes:   maxBatchBytes,
				MaxPendingBytes: 1 * tempo.GiB,
			}, benchDrainOptions{
				consumerDelay: delay,
			})

			var seq atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := int(seq.Add(1))
					if err := d.Enqueue(benchEventPayload(id)); err != nil {
						b.Fatalf("enqueue: %v", err)
					}
				}
			})

			b.StopTimer()
			timeout := slowConsumerDrainTimeout(b.N, maxBatchBytes, benchPayloadBytes(), delay)
			shutdownBenchDispatcher(b, d, timeout)
			waitForDelivered(b, delivered, int64(b.N), timeout)
			reportBenchMetrics(b, delivered, batches)
		})
	}
}

func itoa(v int) string {
	return strconv.Itoa(v)
}
