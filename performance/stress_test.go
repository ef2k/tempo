package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

type soakSample struct {
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	Produced       int64   `json:"produced"`
	Delivered      int64   `json:"delivered"`
	Batches        int64   `json:"batches"`
	Backlog        int64   `json:"backlog"`
	ItemsPerSecond float64 `json:"items_per_second"`
	BatchesPerSec  float64 `json:"batches_per_second"`
	HeapAllocBytes uint64  `json:"heap_alloc_bytes"`
	HeapObjects    uint64  `json:"heap_objects"`
	Goroutines     int     `json:"goroutines"`
	GCCycles       uint32  `json:"gc_cycles"`
}

type soakSnapshot struct {
	StartedAt          time.Time `json:"started_at"`
	Runtime            string    `json:"runtime"`
	SampleInterval     string    `json:"sample_interval"`
	Produced           int64     `json:"produced"`
	Delivered          int64     `json:"delivered"`
	Batches            int64     `json:"batches"`
	FinalBacklog       int64     `json:"final_backlog"`
	PeakBacklog        int64     `json:"peak_backlog"`
	PeakHeapAllocBytes uint64    `json:"peak_heap_alloc_bytes"`
	PeakGoroutines     int       `json:"peak_goroutines"`
	DrainDuration      string    `json:"drain_duration"`
	ShutdownDuration   string    `json:"shutdown_duration"`
}

type soakCollection struct {
	samples            []soakSample
	peakBacklog        int64
	peakHeapAllocBytes uint64
	peakGoroutines     int
	streamErr          error
}

type soakOutputPaths struct {
	snapshotPath        string
	streamPath          string
	displaySnapshotPath string
	displayStreamPath   string
}

func soakSampleInterval(runFor time.Duration) time.Duration {
	if runFor >= 20*time.Second {
		return 5 * time.Second
	}
	if runFor >= 5*time.Second {
		return time.Second
	}
	step := runFor / 4
	if step < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	return step
}

func soakFilenameBase(startedAt time.Time, runtime string) string {
	return fmt.Sprintf(
		"soak-%s-%s",
		startedAt.Format("20060102-150405"),
		strings.ReplaceAll(runtime, string(os.PathSeparator), "-"),
	)
}

func prepareSoakOutputPaths(startedAt time.Time, runtime string) (soakOutputPaths, error) {
	outDir := os.Getenv("TEMPO_SOAK_OUT_DIR")
	displayDir := outDir
	if outDir == "" {
		outDir = "out"
		displayDir = filepath.Join("performance", "out")
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return soakOutputPaths{}, err
	}

	base := soakFilenameBase(startedAt, runtime)
	return soakOutputPaths{
		snapshotPath:        filepath.Join(outDir, base+"-results.json"),
		streamPath:          filepath.Join(outDir, base+".jsonl"),
		displaySnapshotPath: filepath.Join(displayDir, base+"-results.json"),
		displayStreamPath:   filepath.Join(displayDir, base+".jsonl"),
	}, nil
}

func startSoakCollector(
	startedAt time.Time,
	sampleEvery time.Duration,
	produced, delivered, batches *atomic.Int64,
	streamPath string,
) (chan struct{}, chan soakCollection, error) {
	streamFile, err := os.Create(streamPath)
	if err != nil {
		return nil, nil, err
	}

	stop := make(chan struct{})
	done := make(chan soakCollection, 1)

	go func() {
		defer streamFile.Close()

		ticker := time.NewTicker(sampleEvery)
		defer ticker.Stop()
		encoder := json.NewEncoder(streamFile)

		var (
			collection   soakCollection
			prevProduced int64
			prevBatches  int64
			prevSampleAt = startedAt
			recordSample = func(now time.Time) {
				currentProduced := produced.Load()
				currentDelivered := delivered.Load()
				currentBatches := batches.Load()
				backlog := currentProduced - currentDelivered
				if backlog > collection.peakBacklog {
					collection.peakBacklog = backlog
				}

				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				if mem.HeapAlloc > collection.peakHeapAllocBytes {
					collection.peakHeapAllocBytes = mem.HeapAlloc
				}

				goroutines := runtime.NumGoroutine()
				if goroutines > collection.peakGoroutines {
					collection.peakGoroutines = goroutines
				}

				elapsed := now.Sub(prevSampleAt).Seconds()
				itemsPerSecond := 0.0
				batchesPerSecond := 0.0
				if elapsed > 0 {
					itemsPerSecond = float64(currentProduced-prevProduced) / elapsed
					batchesPerSecond = float64(currentBatches-prevBatches) / elapsed
				}

				sample := soakSample{
					ElapsedSeconds: now.Sub(startedAt).Seconds(),
					Produced:       currentProduced,
					Delivered:      currentDelivered,
					Batches:        currentBatches,
					Backlog:        backlog,
					ItemsPerSecond: itemsPerSecond,
					BatchesPerSec:  batchesPerSecond,
					HeapAllocBytes: mem.HeapAlloc,
					HeapObjects:    mem.HeapObjects,
					Goroutines:     goroutines,
					GCCycles:       mem.NumGC,
				}

				collection.samples = append(collection.samples, sample)
				if collection.streamErr == nil {
					if err := encoder.Encode(sample); err != nil {
						collection.streamErr = err
					} else if err := streamFile.Sync(); err != nil {
						collection.streamErr = err
					}
				}

				prevProduced = currentProduced
				prevBatches = currentBatches
				prevSampleAt = now
			}
		)

		for {
			select {
			case now := <-ticker.C:
				recordSample(now)
			case <-stop:
				recordSample(time.Now())
				done <- collection
				return
			}
		}
	}()

	return stop, done, nil
}

func writeSoakSnapshot(snapshot soakSnapshot, path string) error {
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}

	return nil
}

func waitForSettledDelivery(produced, delivered *atomic.Int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if delivered.Load() >= produced.Load() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return delivered.Load() >= produced.Load()
}

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

	startedAt := time.Now()
	runFor := 5 * time.Minute
	if raw := os.Getenv("TEMPO_SOAK_DURATION"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse TEMPO_SOAK_DURATION: %v", err)
		}
		runFor = parsed
	}
	outputPaths, err := prepareSoakOutputPaths(startedAt, runFor.String())
	if err != nil {
		t.Fatalf("prepare soak output paths: %v", err)
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
	var produced atomic.Int64
	var delivered atomic.Int64
	var batches atomic.Int64

	sampleEvery := soakSampleInterval(runFor)
	stopSampling, samplingDone, err := startSoakCollector(
		startedAt,
		sampleEvery,
		&produced,
		&delivered,
		&batches,
		outputPaths.streamPath,
	)
	if err != nil {
		t.Fatalf("start soak collector: %v", err)
	}

	fmt.Printf(
		"\nsoak output\n  stream: %s\n  snapshot: %s\n\n",
		outputPaths.displayStreamPath,
		outputPaths.displaySnapshotPath,
	)

	stopConsumers := make(chan struct{})
	go func() {
		for {
			select {
			case batch := <-d.Batches():
				delivered.Add(int64(len(batch)))
				batches.Add(1)
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
					produced.Add(1)
					seq++
				}
			}
		}(i)
	}

	time.Sleep(runFor)
	drainStart := time.Now()
	close(stopProducing)
	producers.Wait()

	shutdownStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after soak run: %v", err)
	}
	shutdownDuration := time.Since(shutdownStart)
	drainDuration := time.Since(drainStart)

	if !waitForSettledDelivery(&produced, &delivered, time.Second) {
		t.Fatalf(
			"expected soak run to settle after shutdown: produced=%d delivered=%d",
			produced.Load(),
			delivered.Load(),
		)
	}

	close(stopConsumers)
	close(stopSampling)
	collection := <-samplingDone
	if collection.streamErr != nil {
		t.Fatalf("stream soak samples: %v", collection.streamErr)
	}

	totalProduced := produced.Load()
	totalDelivered := delivered.Load()
	finalBacklog := totalProduced - totalDelivered

	if totalDelivered == 0 {
		t.Fatal("expected soak run to deliver some items")
	}
	if finalBacklog != 0 {
		t.Fatalf("expected soak run to drain fully: backlog=%d produced=%d delivered=%d", finalBacklog, totalProduced, totalDelivered)
	}

	endGoroutines := runtime.NumGoroutine()
	if endGoroutines > startGoroutines+16 {
		t.Fatalf("unexpected goroutine growth during soak run: start=%d end=%d", startGoroutines, endGoroutines)
	}

	snapshot := soakSnapshot{
		StartedAt:          startedAt,
		Runtime:            runFor.String(),
		SampleInterval:     sampleEvery.String(),
		Produced:           totalProduced,
		Delivered:          totalDelivered,
		Batches:            batches.Load(),
		FinalBacklog:       finalBacklog,
		PeakBacklog:        collection.peakBacklog,
		PeakHeapAllocBytes: collection.peakHeapAllocBytes,
		PeakGoroutines:     collection.peakGoroutines,
		DrainDuration:      drainDuration.String(),
		ShutdownDuration:   shutdownDuration.String(),
	}

	if err := writeSoakSnapshot(snapshot, outputPaths.snapshotPath); err != nil {
		t.Fatalf("write soak snapshot: %v", err)
	}

	fmt.Printf(
		"\nsoak summary\n  stream: %s\n  snapshot: %s\n  runtime: %s\n  sample interval: %s\n  produced: %d\n  delivered: %d\n  batches: %d\n  peak backlog: %d\n  peak heap alloc: %d bytes\n  peak goroutines: %d\n  drain duration: %s\n  shutdown duration: %s\n",
		outputPaths.displayStreamPath,
		outputPaths.displaySnapshotPath,
		snapshot.Runtime,
		snapshot.SampleInterval,
		snapshot.Produced,
		snapshot.Delivered,
		snapshot.Batches,
		snapshot.PeakBacklog,
		snapshot.PeakHeapAllocBytes,
		snapshot.PeakGoroutines,
		snapshot.DrainDuration,
		snapshot.ShutdownDuration,
	)
}
