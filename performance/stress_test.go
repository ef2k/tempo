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
	StartedAt          time.Time       `json:"started_at"`
	Runtime            string          `json:"runtime"`
	SampleInterval     string          `json:"sample_interval"`
	Environment        soakEnvironment `json:"environment"`
	Config             soakConfig      `json:"config"`
	Produced           int64           `json:"produced"`
	Delivered          int64           `json:"delivered"`
	Batches            int64           `json:"batches"`
	AvgItemsPerSec     float64         `json:"avg_items_per_second"`
	MinItemsPerSec     float64         `json:"min_items_per_second"`
	MaxItemsPerSec     float64         `json:"max_items_per_second"`
	AvgBatchesPerSec   float64         `json:"avg_batches_per_second"`
	MinBatchesPerSec   float64         `json:"min_batches_per_second"`
	MaxBatchesPerSec   float64         `json:"max_batches_per_second"`
	ThroughputRangePct float64         `json:"throughput_range_percent"`
	FinalBacklog       int64           `json:"final_backlog"`
	PeakBacklog        int64           `json:"peak_backlog"`
	PeakHeapAllocBytes uint64          `json:"peak_heap_alloc_bytes"`
	FinalHeapAlloc     uint64          `json:"final_heap_alloc_bytes"`
	PeakGoroutines     int             `json:"peak_goroutines"`
	FinalGoroutines    int             `json:"final_goroutines"`
	DrainDuration      string          `json:"drain_duration"`
	ShutdownDuration   string          `json:"shutdown_duration"`
	Assessment         soakAssessment  `json:"assessment"`
}

type soakEnvironment struct {
	GoOS      string `json:"goos"`
	GoArch    string `json:"goarch"`
	GoVersion string `json:"go_version"`
	CPUCount  int    `json:"cpu_count"`
}

type soakConfig struct {
	Interval      string `json:"interval"`
	MaxBatchItems int    `json:"max_batch_items"`
	NumProducers  int    `json:"num_producers"`
	ConsumerDelay string `json:"consumer_delay"`
	DrainTimeout  string `json:"drain_timeout"`
}

type assessmentStatus string

const (
	assessmentPass assessmentStatus = "pass"
	assessmentWarn assessmentStatus = "warn"
	assessmentFail assessmentStatus = "fail"
)

type soakAssessment struct {
	StartedAt             time.Time             `json:"started_at"`
	Runtime               string                `json:"runtime"`
	StreamPath            string                `json:"stream_path"`
	ResultsPath           string                `json:"results_path"`
	ObservedItemsPerSec   float64               `json:"observed_items_per_second"`
	ObservedBatchesPerSec float64               `json:"observed_batches_per_second"`
	ThroughputRangePct    float64               `json:"throughput_range_percent"`
	Acceptance           soakAssessmentSection `json:"acceptance"`
	Observations         soakAssessmentSection `json:"observations"`
	Correctness           soakAssessmentSection `json:"correctness"`
	Throughput            soakAssessmentSection `json:"throughput"`
	Backlog               soakAssessmentSection `json:"backlog"`
	Memory                soakAssessmentSection `json:"memory"`
	Goroutines            soakAssessmentSection `json:"goroutines"`
	Drain                 soakAssessmentSection `json:"drain"`
	Overall               soakAssessmentSection `json:"overall"`
}

type soakAssessmentSection struct {
	Status assessmentStatus `json:"status"`
	Notes  []string         `json:"notes"`
}

type soakCollection struct {
	samples            []soakSample
	peakBacklog        int64
	peakHeapAllocBytes uint64
	peakGoroutines     int
	streamErr          error
}

type soakStreamHeader struct {
	Type           string          `json:"type"`
	StartedAt      time.Time       `json:"started_at"`
	Runtime        string          `json:"runtime"`
	SampleInterval string          `json:"sample_interval"`
	Environment    soakEnvironment `json:"environment"`
	Config         soakConfig      `json:"config"`
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
	runFor time.Duration,
	sampleEvery time.Duration,
	environment soakEnvironment,
	config soakConfig,
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
		header := soakStreamHeader{
			Type:           "run_header",
			StartedAt:      startedAt,
			Runtime:        runFor.String(),
			SampleInterval: sampleEvery.String(),
			Environment:    environment,
			Config:         config,
		}
		if err := encoder.Encode(header); err != nil {
			done <- soakCollection{streamErr: err}
			return
		}
		if err := streamFile.Sync(); err != nil {
			done <- soakCollection{streamErr: err}
			return
		}

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

func activeSoakSamples(samples []soakSample) []soakSample {
	if len(samples) <= 1 {
		return samples
	}
	return samples[:len(samples)-1]
}

func throughputStats(samples []soakSample) (avgItems, avgBatches, minItems, maxItems, minBatches, maxBatches float64) {
	if len(samples) == 0 {
		return 0, 0, 0, 0, 0, 0
	}
	minItems = samples[0].ItemsPerSecond
	maxItems = samples[0].ItemsPerSecond
	minBatches = samples[0].BatchesPerSec
	maxBatches = samples[0].BatchesPerSec
	for _, sample := range samples {
		avgItems += sample.ItemsPerSecond
		avgBatches += sample.BatchesPerSec
		if sample.ItemsPerSecond < minItems {
			minItems = sample.ItemsPerSecond
		}
		if sample.ItemsPerSecond > maxItems {
			maxItems = sample.ItemsPerSecond
		}
		if sample.BatchesPerSec < minBatches {
			minBatches = sample.BatchesPerSec
		}
		if sample.BatchesPerSec > maxBatches {
			maxBatches = sample.BatchesPerSec
		}
	}
	avgItems /= float64(len(samples))
	avgBatches /= float64(len(samples))
	return avgItems, avgBatches, minItems, maxItems, minBatches, maxBatches
}

func heapRecovered(samples []soakSample, peak uint64) bool {
	if peak == 0 {
		return false
	}
	for _, sample := range samples {
		if sample.HeapAllocBytes <= peak/2 {
			return true
		}
	}
	return false
}

func makeAssessment(snapshot soakSnapshot, samples []soakSample, startGoroutines int, outputPaths soakOutputPaths) soakAssessment {
	active := activeSoakSamples(samples)
	correctness := soakAssessmentSection{Status: assessmentPass}
	if snapshot.Produced != snapshot.Delivered || snapshot.FinalBacklog != 0 {
		correctness.Status = assessmentFail
	}
	correctness.Notes = []string{
		fmt.Sprintf("produced=%d delivered=%d", snapshot.Produced, snapshot.Delivered),
		fmt.Sprintf("final backlog=%d", snapshot.FinalBacklog),
	}

	throughput := soakAssessmentSection{Status: assessmentPass}
	switch {
	case snapshot.AvgItemsPerSec == 0:
		throughput.Status = assessmentFail
	case snapshot.ThroughputRangePct > 25:
		throughput.Status = assessmentFail
	case snapshot.ThroughputRangePct > 10:
		throughput.Status = assessmentWarn
	}
	throughput.Notes = []string{
		fmt.Sprintf("avg items/sec=%.0f", snapshot.AvgItemsPerSec),
		fmt.Sprintf("avg batches/sec=%.0f", snapshot.AvgBatchesPerSec),
		fmt.Sprintf("throughput range=%.2f%%", snapshot.ThroughputRangePct),
	}

	backlog := soakAssessmentSection{Status: assessmentPass}
	switch {
	case snapshot.FinalBacklog != 0:
		backlog.Status = assessmentFail
	case snapshot.PeakBacklog > int64(snapshot.Config.MaxBatchItems*2):
		backlog.Status = assessmentWarn
	}
	backlog.Notes = []string{
		fmt.Sprintf("peak backlog=%d", snapshot.PeakBacklog),
		fmt.Sprintf("max batch items=%d", snapshot.Config.MaxBatchItems),
	}

	memory := soakAssessmentSection{Status: assessmentPass}
	switch {
	case snapshot.PeakHeapAllocBytes > 64<<20:
		memory.Status = assessmentFail
	case !heapRecovered(active, snapshot.PeakHeapAllocBytes):
		memory.Status = assessmentWarn
	}
	var gcCycles uint32
	if len(samples) > 0 {
		gcCycles = samples[len(samples)-1].GCCycles
	}
	memory.Notes = []string{
		fmt.Sprintf("peak heap alloc=%d bytes", snapshot.PeakHeapAllocBytes),
		fmt.Sprintf("final heap alloc=%d bytes", snapshot.FinalHeapAlloc),
		fmt.Sprintf("gc cycles observed=%d", gcCycles),
	}

	goroutines := soakAssessmentSection{Status: assessmentPass}
	switch {
	case snapshot.FinalGoroutines > startGoroutines+16:
		goroutines.Status = assessmentFail
	case snapshot.PeakGoroutines > startGoroutines+snapshot.Config.NumProducers+8:
		goroutines.Status = assessmentWarn
	}
	goroutines.Notes = []string{
		fmt.Sprintf("start goroutines=%d", startGoroutines),
		fmt.Sprintf("peak goroutines=%d", snapshot.PeakGoroutines),
		fmt.Sprintf("final goroutines=%d", snapshot.FinalGoroutines),
	}

	drain := soakAssessmentSection{Status: assessmentPass}
	drainDuration, _ := time.ParseDuration(snapshot.DrainDuration)
	shutdownDuration, _ := time.ParseDuration(snapshot.ShutdownDuration)
	drainTimeout, _ := time.ParseDuration(snapshot.Config.DrainTimeout)
	if drainTimeout <= 0 {
		drainTimeout = 5 * time.Second
	}
	switch {
	case drainDuration > drainTimeout || shutdownDuration > drainTimeout:
		drain.Status = assessmentFail
	case drainDuration > drainTimeout/2 || shutdownDuration > drainTimeout/2:
		drain.Status = assessmentWarn
	}
	drain.Notes = []string{
		fmt.Sprintf("drain duration=%s", snapshot.DrainDuration),
		fmt.Sprintf("shutdown duration=%s", snapshot.ShutdownDuration),
		fmt.Sprintf("drain timeout=%s", snapshot.Config.DrainTimeout),
	}

	acceptance := soakAssessmentSection{Status: assessmentPass}
	for _, section := range []soakAssessmentSection{correctness, goroutines, drain} {
		if section.Status == assessmentFail {
			acceptance.Status = assessmentFail
			break
		}
		if section.Status == assessmentWarn {
			acceptance.Status = assessmentWarn
		}
	}
	switch acceptance.Status {
	case assessmentPass:
		acceptance.Notes = []string{"tempo stayed live under intentional backpressure and recovered without losing accepted items"}
	case assessmentWarn:
		acceptance.Notes = []string{"tempo recovered, but one or more acceptance signals need a closer look"}
	default:
		acceptance.Notes = []string{"tempo did not meet the recovery and correctness acceptance criteria for this soak run"}
	}

	observations := soakAssessmentSection{Status: assessmentPass}
	for _, section := range []soakAssessmentSection{throughput, backlog, memory} {
		if section.Status == assessmentFail {
			observations.Status = assessmentFail
			break
		}
		if section.Status == assessmentWarn {
			observations.Status = assessmentWarn
		}
	}
	switch observations.Status {
	case assessmentPass:
		observations.Notes = []string{"resource and throughput observations stayed within the current advisory thresholds"}
	case assessmentWarn:
		observations.Notes = []string{"the soak run recovered, but advisory resource or throughput observations deserve review"}
	default:
		observations.Notes = []string{"the soak run recovered, but advisory resource or throughput observations were well outside the expected pre-limit range"}
	}

	overall := soakAssessmentSection{Status: acceptance.Status}
	for _, section := range []soakAssessmentSection{acceptance, observations} {
		if section.Status == assessmentFail {
			overall.Status = assessmentFail
			break
		}
		if section.Status == assessmentWarn {
			overall.Status = assessmentWarn
		}
	}
	switch overall.Status {
	case assessmentPass:
		overall.Notes = []string{"the soak run looks healthy across correctness, throughput, backlog, memory, goroutines, and drain behavior"}
	case assessmentWarn:
		overall.Notes = []string{"the soak run completed, but one or more categories need a closer look"}
	default:
		overall.Notes = []string{"the soak run exposed at least one category that does not meet the expected bar"}
	}

	return soakAssessment{
		StartedAt:             snapshot.StartedAt,
		Runtime:               snapshot.Runtime,
		StreamPath:            outputPaths.displayStreamPath,
		ResultsPath:           outputPaths.displaySnapshotPath,
		ObservedItemsPerSec:   snapshot.AvgItemsPerSec,
		ObservedBatchesPerSec: snapshot.AvgBatchesPerSec,
		ThroughputRangePct:    snapshot.ThroughputRangePct,
		Acceptance:            acceptance,
		Observations:          observations,
		Correctness:           correctness,
		Throughput:            throughput,
		Backlog:               backlog,
		Memory:                memory,
		Goroutines:            goroutines,
		Drain:                 drain,
		Overall:               overall,
	}
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

// TestSoakSustainedLoadStaysHealthy proves that Tempo can absorb temporary
// consumer-side backpressure, remain live while backlog accumulates, and then
// recover without losing accepted items once the artificial slowdown is lifted.
//
// Acceptance for this soak is intentionally centered on recovery and
// correctness:
// - Tempo stays live during the pressure window.
// - Tempo drains all accepted items after pressure is removed.
// - Shutdown completes within the configured drain timeout.
//
// Throughput, peak backlog, and peak memory are still recorded, but for the
// current unbounded design they are advisory observations rather than the
// primary pass/fail signal for this recovery test.
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

	const numProducers = 32
	config := tempo.Config{
		Interval:      10 * time.Millisecond,
		MaxBatchItems: 128,
	}
	consumerDelay := time.Duration(0)
	if raw := os.Getenv("TEMPO_SOAK_CONSUMER_DELAY"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse TEMPO_SOAK_CONSUMER_DELAY: %v", err)
		}
		consumerDelay = parsed
	}
	drainTimeout := runFor
	if raw := os.Getenv("TEMPO_SOAK_DRAIN_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse TEMPO_SOAK_DRAIN_TIMEOUT: %v", err)
		}
		drainTimeout = parsed
	}
	environment := soakEnvironment{
		GoOS:      runtime.GOOS,
		GoArch:    runtime.GOARCH,
		GoVersion: runtime.Version(),
		CPUCount:  runtime.NumCPU(),
	}
	runConfig := soakConfig{
		Interval:      config.Interval.String(),
		MaxBatchItems: config.MaxBatchItems,
		NumProducers:  numProducers,
		ConsumerDelay: consumerDelay.String(),
		DrainTimeout:  drainTimeout.String(),
	}

	d, err := tempo.NewDispatcher(&config)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	go d.Start()

	startGoroutines := runtime.NumGoroutine()
	var produced atomic.Int64
	var delivered atomic.Int64
	var batches atomic.Int64

	sampleEvery := soakSampleInterval(runFor)
	stopSampling, samplingDone, err := startSoakCollector(
		startedAt,
		runFor,
		sampleEvery,
		environment,
		runConfig,
		&produced,
		&delivered,
		&batches,
		outputPaths.streamPath,
	)
	if err != nil {
		t.Fatalf("start soak collector: %v", err)
	}

	fmt.Printf(
		"\nsoak output\n  stream: %s\n  results: %s\n\n",
		outputPaths.displayStreamPath,
		outputPaths.displaySnapshotPath,
	)

	stopConsumers := make(chan struct{})
	var applyConsumerDelay atomic.Bool
	applyConsumerDelay.Store(consumerDelay > 0)
	go func() {
		for {
			select {
			case batch := <-d.Batches():
				if applyConsumerDelay.Load() && consumerDelay > 0 {
					time.Sleep(consumerDelay)
				}
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
	applyConsumerDelay.Store(false)

	shutdownStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := d.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown after soak run: %v", err)
	}
	shutdownDuration := time.Since(shutdownStart)
	drainDuration := time.Since(drainStart)

	if !waitForSettledDelivery(&produced, &delivered, 5*time.Second) {
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
	active := activeSoakSamples(collection.samples)
	avgItems, avgBatches, minItems, maxItems, minBatches, maxBatches := throughputStats(active)
	rangePct := 0.0
	if avgItems > 0 {
		rangePct = ((maxItems - minItems) / avgItems) * 100
	}

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
		Environment:        environment,
		Config:             runConfig,
		Produced:           totalProduced,
		Delivered:          totalDelivered,
		Batches:            batches.Load(),
		AvgItemsPerSec:     avgItems,
		MinItemsPerSec:     minItems,
		MaxItemsPerSec:     maxItems,
		AvgBatchesPerSec:   avgBatches,
		MinBatchesPerSec:   minBatches,
		MaxBatchesPerSec:   maxBatches,
		ThroughputRangePct: rangePct,
		FinalBacklog:       finalBacklog,
		PeakBacklog:        collection.peakBacklog,
		PeakHeapAllocBytes: collection.peakHeapAllocBytes,
		FinalHeapAlloc:     collection.samples[len(collection.samples)-1].HeapAllocBytes,
		PeakGoroutines:     collection.peakGoroutines,
		FinalGoroutines:    endGoroutines,
		DrainDuration:      drainDuration.String(),
		ShutdownDuration:   shutdownDuration.String(),
	}

	assessment := makeAssessment(snapshot, collection.samples, startGoroutines, outputPaths)
	snapshot.Assessment = assessment
	if err := writeSoakSnapshot(snapshot, outputPaths.snapshotPath); err != nil {
		t.Fatalf("write soak snapshot: %v", err)
	}

	fmt.Printf(
		"\nsoak summary\n  stream: %s\n  results: %s\n  runtime: %s\n  sample interval: %s\n  consumer delay: %s\n  drain timeout: %s\n  acceptance: %s\n  observations: %s\n  produced: %d\n  delivered: %d\n  batches: %d\n  avg items/sec: %.0f\n  min items/sec: %.0f\n  max items/sec: %.0f\n  avg batches/sec: %.0f\n  throughput range: %.2f%%\n  peak backlog: %d\n  peak heap alloc: %d bytes\n  peak goroutines: %d\n  drain duration: %s\n  shutdown duration: %s\n  correctness: %s\n  throughput: %s\n  backlog: %s\n  memory: %s\n  goroutines: %s\n  drain: %s\n  overall: %s\n",
		outputPaths.displayStreamPath,
		outputPaths.displaySnapshotPath,
		snapshot.Runtime,
		snapshot.SampleInterval,
		runConfig.ConsumerDelay,
		runConfig.DrainTimeout,
		assessment.Acceptance.Status,
		assessment.Observations.Status,
		snapshot.Produced,
		snapshot.Delivered,
		snapshot.Batches,
		snapshot.AvgItemsPerSec,
		snapshot.MinItemsPerSec,
		snapshot.MaxItemsPerSec,
		snapshot.AvgBatchesPerSec,
		snapshot.ThroughputRangePct,
		snapshot.PeakBacklog,
		snapshot.PeakHeapAllocBytes,
		snapshot.PeakGoroutines,
		snapshot.DrainDuration,
		snapshot.ShutdownDuration,
		assessment.Correctness.Status,
		assessment.Throughput.Status,
		assessment.Backlog.Status,
		assessment.Memory.Status,
		assessment.Goroutines.Status,
		assessment.Drain.Status,
		assessment.Overall.Status,
	)
}
