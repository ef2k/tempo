package tuner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tempo "github.com/ef2k/tempo"
)

const (
	KiB int64 = 1024
	MiB       = 1024 * KiB
	GiB       = 1024 * MiB

	maxTuneProbeRuns = 12
)

type Options struct {
	PayloadBytes  int64
	ProbeDuration time.Duration
}

type ProbeRun struct {
	ProducerCount    int
	ConsumerDelay    time.Duration
	MaxBufferedBytes int64
	MaxBatchBytes    int64
	ItemsPerSec      float64
	BytesPerSec      float64
	PeakHeapBytes    uint64
	PeakBuffered     int64
	Rejections       int64
	RejectionRate    float64
	Healthy          bool
}

type Recommendation struct {
	TotalMemoryBytes        uint64
	PayloadBytes            int64
	ProbeDuration           time.Duration
	ProducerCount           int
	ObservedItemsPerSec     float64
	ObservedBytesPerSec     float64
	ObservedPeakHeapBytes   uint64
	ObservedPeakBuffered    int64
	RecommendedDelay        time.Duration
	RecommendedMaxBuffered  int64
	RecommendedMaxBatch     int64
	BufferedItemCapacity    int64
	EstimatedBurstWindow    time.Duration
	Rejections              int64
	BatchShapingRecommended bool
	Calibrated              bool
	EdgeFound               bool
	FailureBufferedBytes    int64
	Runs                    []ProbeRun
	Notes                   []string
}

type probeConfig struct {
	PayloadBytes     int64
	ProbeDuration    time.Duration
	ProducerCount    int
	MaxBufferedBytes int64
	MaxBatchBytes    int64
	ConsumerDelay    time.Duration
}

type probeResult struct {
	itemsPerSec       float64
	bytesPerSec       float64
	peakHeapBytes     uint64
	peakBufferedBytes int64
	rejections        int64
	rejectionRate     float64
	timedOut          bool
}

var errTuneProbeBudgetExhausted = errors.New("tune probe budget exhausted")

func Tune(ctx context.Context, opts Options) (Recommendation, error) {
	if opts.PayloadBytes <= 0 {
		return Recommendation{}, errors.New("payload bytes must be greater than zero")
	}
	if opts.ProbeDuration <= 0 {
		opts.ProbeDuration = 5 * time.Second
	}

	totalMem, _ := detectTotalMemoryBytes()
	producers := recommendedProducerCount()
	startBuffered := initialBufferedBudget(totalMem, opts.PayloadBytes)
	maxBuffered := maxBufferedBudget(totalMem, opts.PayloadBytes)
	minBuffered := minimumBufferedBudget(opts.PayloadBytes)

	recommendation := Recommendation{
		TotalMemoryBytes: totalMem,
		PayloadBytes:     opts.PayloadBytes,
		ProbeDuration:    opts.ProbeDuration,
	}

	delayCandidates := probeDelayCandidates()
	var chosenPass *ProbeRun
	var fallbackPass *ProbeRun
	var failureRun *ProbeRun
	var edgeFound bool
	remainingRuns := maxTuneProbeRuns

	for _, producerCount := range probeProducerCandidates(producers) {
		for _, delay := range delayCandidates {
			lastHealthy, firstFailure, runs, err := searchDelayProfile(ctx, opts, producerCount, delay, startBuffered, minBuffered, maxBuffered, &remainingRuns)
			recommendation.Runs = append(recommendation.Runs, runs...)
			if err != nil {
				if errors.Is(err, errTuneProbeBudgetExhausted) {
					break
				}
				return Recommendation{}, err
			}
			if lastHealthy != nil {
				run := *lastHealthy
				fallbackPass = &run
			}
			if lastHealthy == nil || firstFailure == nil {
				continue
			}
			if lastHealthy.ConsumerDelay <= 0 {
				continue
			}
			chosenPass = lastHealthy
			run := *firstFailure
			failureRun = &run
			edgeFound = true
			break
		}
		if edgeFound || remainingRuns <= 0 {
			break
		}
	}

	displayRun := chosenPass
	if displayRun == nil {
		displayRun = fallbackPass
	}
	if displayRun == nil {
		return Recommendation{}, errors.New("could not find a healthy probe configuration")
	}

	recommendedBatch := recommendBatchBytes(opts.PayloadBytes, displayRun.MaxBufferedBytes)
	batchRecommended := recommendedBatch > 0
	bufferedItems := displayRun.MaxBufferedBytes / opts.PayloadBytes

	burstWindow := time.Duration(0)
	if displayRun.BytesPerSec > 0 {
		seconds := float64(displayRun.MaxBufferedBytes) / displayRun.BytesPerSec
		burstWindow = time.Duration(seconds * float64(time.Second))
	}

	recommendation.ObservedItemsPerSec = displayRun.ItemsPerSec
	recommendation.ObservedBytesPerSec = displayRun.BytesPerSec
	recommendation.ObservedPeakHeapBytes = displayRun.PeakHeapBytes
	recommendation.ObservedPeakBuffered = displayRun.PeakBuffered
	recommendation.ProducerCount = displayRun.ProducerCount
	recommendation.RecommendedDelay = displayRun.ConsumerDelay
	recommendation.RecommendedMaxBuffered = displayRun.MaxBufferedBytes
	recommendation.RecommendedMaxBatch = recommendedBatch
	recommendation.BufferedItemCapacity = bufferedItems
	recommendation.EstimatedBurstWindow = burstWindow
	recommendation.Rejections = displayRun.Rejections
	recommendation.BatchShapingRecommended = batchRecommended
	recommendation.Calibrated = edgeFound
	recommendation.EdgeFound = edgeFound
	if failureRun != nil {
		recommendation.FailureBufferedBytes = failureRun.MaxBufferedBytes
	}

	notes := []string{
		"MaxBufferedBytes is the primary safety boundary.",
		"MaxBatchBytes is optional shaping; leave it unset if downstream batch size does not matter.",
	}
	if batchRecommended {
		notes = append(notes, "Recommended MaxBatchBytes is sized to fit several payloads without becoming brittle for single large items.")
	} else {
		notes = append(notes, "Payloads are large enough that batch-size shaping is not recommended by default on this machine.")
	}
	if edgeFound {
		notes = append(notes, "Recommendation is the last healthy point before queue pressure appeared.")
	} else {
		notes = append(notes, "The probe did not reach queue pressure within the tune budget, so these settings are a provisional clean starting point.")
		notes = append(notes, "Try a longer probe duration or a larger payload size if you want the tuner to validate the boundary more aggressively.")
		if remainingRuns <= 0 {
			notes = append(notes, "The tune run stopped after reaching its probe budget to keep runtime bounded.")
		}
	}
	if totalMem > 0 {
		notes = append(notes, fmt.Sprintf("Recommendation stays within a conservative fraction of detected machine memory (%s total).", formatBytes(int64(totalMem))))
	}
	recommendation.Notes = notes

	return recommendation, nil
}

func searchDelayProfile(ctx context.Context, opts Options, producers int, delay time.Duration, startBuffered, minBuffered, maxBuffered int64, remainingRuns *int) (*ProbeRun, *ProbeRun, []ProbeRun, error) {
	current := startBuffered
	var lastHealthy *ProbeRun
	var firstFailure *ProbeRun
	runs := make([]ProbeRun, 0, 8)

	run, err := runBudgetedProbeStep(ctx, probeConfig{
		PayloadBytes:     opts.PayloadBytes,
		ProbeDuration:    opts.ProbeDuration,
		ProducerCount:    producers,
		MaxBufferedBytes: current,
		MaxBatchBytes:    probeBatchBytes(opts.PayloadBytes, current),
		ConsumerDelay:    delay,
	}, remainingRuns)
	if err != nil {
		return nil, nil, runs, err
	}
	runs = append(runs, run)

	if run.Healthy {
		copyRun := run
		lastHealthy = &copyRun
		for current > minBuffered {
			next := roundToMiB(current / 2)
			if next < minBuffered {
				next = minBuffered
			}
			if next == current {
				break
			}
			run, err := runBudgetedProbeStep(ctx, probeConfig{
				PayloadBytes:     opts.PayloadBytes,
				ProbeDuration:    opts.ProbeDuration,
				ProducerCount:    producers,
				MaxBufferedBytes: next,
				MaxBatchBytes:    probeBatchBytes(opts.PayloadBytes, next),
				ConsumerDelay:    delay,
			}, remainingRuns)
			if err != nil {
				return nil, nil, runs, err
			}
			runs = append(runs, run)
			if run.Healthy {
				copyRun := run
				lastHealthy = &copyRun
				current = next
				continue
			}
			copyRun := run
			firstFailure = &copyRun
			break
		}
	} else {
		copyRun := run
		firstFailure = &copyRun
		for current < maxBuffered {
			next := roundToMiB(current * 2)
			if next > maxBuffered {
				next = maxBuffered
			}
			if next == current {
				break
			}
			run, err := runBudgetedProbeStep(ctx, probeConfig{
				PayloadBytes:     opts.PayloadBytes,
				ProbeDuration:    opts.ProbeDuration,
				ProducerCount:    producers,
				MaxBufferedBytes: next,
				MaxBatchBytes:    probeBatchBytes(opts.PayloadBytes, next),
				ConsumerDelay:    delay,
			}, remainingRuns)
			if err != nil {
				return nil, nil, runs, err
			}
			runs = append(runs, run)
			if run.Healthy {
				copyRun := run
				lastHealthy = &copyRun
				current = next
				break
			}
			copyRun := run
			firstFailure = &copyRun
			current = next
		}
	}

	if lastHealthy == nil {
		return nil, firstFailure, runs, nil
	}
	if firstFailure == nil {
		return lastHealthy, nil, runs, nil
	}

	low := lastHealthy.MaxBufferedBytes
	high := firstFailure.MaxBufferedBytes
	if low < high {
		low, high = high, low
	}

	for i := 0; i < 2; i++ {
		if low-high <= 1*MiB {
			break
		}
		mid := roundToMiB((low + high) / 2)
		if mid >= low {
			mid = roundToMiB(low - 1*MiB)
		}
		if mid <= high {
			break
		}
		run, err := runBudgetedProbeStep(ctx, probeConfig{
			PayloadBytes:     opts.PayloadBytes,
			ProbeDuration:    opts.ProbeDuration,
			ProducerCount:    producers,
			MaxBufferedBytes: mid,
			MaxBatchBytes:    probeBatchBytes(opts.PayloadBytes, mid),
			ConsumerDelay:    delay,
		}, remainingRuns)
		if err != nil {
			return nil, nil, runs, err
		}
		runs = append(runs, run)
		if run.Healthy {
			copyRun := run
			lastHealthy = &copyRun
			low = mid
			continue
		}
		copyRun := run
		firstFailure = &copyRun
		high = mid
	}

	return lastHealthy, firstFailure, runs, nil
}

func runBudgetedProbeStep(ctx context.Context, cfg probeConfig, remainingRuns *int) (ProbeRun, error) {
	if remainingRuns != nil {
		if *remainingRuns <= 0 {
			return ProbeRun{}, errTuneProbeBudgetExhausted
		}
		*remainingRuns--
	}
	return runProbeStep(ctx, cfg)
}

func runProbeStep(ctx context.Context, cfg probeConfig) (ProbeRun, error) {
	result, err := runProbe(ctx, cfg)
	if err != nil {
		return ProbeRun{}, err
	}

	healthy := result.rejections == 0 && !result.timedOut
	return ProbeRun{
		ProducerCount:    cfg.ProducerCount,
		ConsumerDelay:    cfg.ConsumerDelay,
		MaxBufferedBytes: cfg.MaxBufferedBytes,
		MaxBatchBytes:    cfg.MaxBatchBytes,
		ItemsPerSec:      result.itemsPerSec,
		BytesPerSec:      result.bytesPerSec,
		PeakHeapBytes:    result.peakHeapBytes,
		PeakBuffered:     result.peakBufferedBytes,
		Rejections:       result.rejections,
		RejectionRate:    result.rejectionRate,
		Healthy:          healthy,
	}, nil
}

func runProbe(ctx context.Context, cfg probeConfig) (probeResult, error) {
	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:         5 * time.Millisecond,
		MaxBatchBytes:    cfg.MaxBatchBytes,
		MaxBufferedBytes: cfg.MaxBufferedBytes,
	})
	if err != nil {
		return probeResult{}, err
	}
	go d.Start()

	payload := make([]byte, cfg.PayloadBytes)
	for i := range payload {
		payload[i] = 'x'
	}

	var produced atomic.Int64
	var delivered atomic.Int64
	var rejectedCount atomic.Int64
	var peakBuffered atomic.Int64

	stopConsumers := make(chan struct{})
	var consumerWG sync.WaitGroup
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		for {
			select {
			case batch := <-d.Batches():
				delivered.Add(int64(len(batch)))
				if cfg.ConsumerDelay > 0 {
					units := (len(batch) + 7) / 8
					time.Sleep(cfg.ConsumerDelay * time.Duration(units))
				}
				currentBuffered := produced.Load() - delivered.Load()
				for {
					peak := peakBuffered.Load()
					if currentBuffered <= peak || peakBuffered.CompareAndSwap(peak, currentBuffered) {
						break
					}
				}
			case <-stopConsumers:
				return
			}
		}
	}()

	stopProducers := make(chan struct{})
	var producerWG sync.WaitGroup
	producerWG.Add(cfg.ProducerCount)
	for i := 0; i < cfg.ProducerCount; i++ {
		go func() {
			defer producerWG.Done()
			for {
				select {
				case <-stopProducers:
					return
				default:
				}

				if err := d.Enqueue(payload); err != nil {
					if err == tempo.ErrQueueFull {
						rejectedCount.Add(1)
						runtime.Gosched()
						continue
					}
					return
				}
				produced.Add(1)
			}
		}()
	}

	var peakHeap uint64
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		var mem runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&mem)
				if mem.HeapAlloc > peakHeap {
					peakHeap = mem.HeapAlloc
				}
			case <-stopProducers:
				runtime.ReadMemStats(&mem)
				if mem.HeapAlloc > peakHeap {
					peakHeap = mem.HeapAlloc
				}
				return
			}
		}
	}()

	timer := time.NewTimer(cfg.ProbeDuration)
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
	}

	close(stopProducers)
	producerWG.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), max(2*time.Second, cfg.ProbeDuration*2))
	defer cancel()
	shutdownErr := d.Shutdown(shutdownCtx)
	close(stopConsumers)
	consumerWG.Wait()
	samplerWG.Wait()

	durationSeconds := cfg.ProbeDuration.Seconds()
	if durationSeconds <= 0 {
		durationSeconds = 1
	}
	itemsPerSec := float64(delivered.Load()) / durationSeconds
	attempts := delivered.Load() + rejectedCount.Load()
	rejectionRate := 0.0
	if attempts > 0 {
		rejectionRate = float64(rejectedCount.Load()) / float64(attempts)
	}
	result := probeResult{
		itemsPerSec:       itemsPerSec,
		bytesPerSec:       itemsPerSec * float64(cfg.PayloadBytes),
		peakHeapBytes:     peakHeap,
		peakBufferedBytes: peakBuffered.Load() * cfg.PayloadBytes,
		rejections:        rejectedCount.Load(),
		rejectionRate:     rejectionRate,
	}
	if shutdownErr != nil {
		if errors.Is(shutdownErr, context.DeadlineExceeded) {
			result.timedOut = true
			return result, nil
		}
		if err == nil {
			err = shutdownErr
		}
	}
	if err != nil {
		return probeResult{}, err
	}
	return result, nil
}

func recommendedProducerCount() int {
	n := runtime.NumCPU() * 4
	if n < 4 {
		return 4
	}
	if n > 64 {
		return 64
	}
	return n
}

func probeProducerCandidates(base int) []int {
	candidates := []int{
		1,
		2,
		maxInt(1, base/2),
		base,
	}
	if base < 256 {
		candidates = append(candidates, minInt(base*2, 256))
	}
	if base < 256 {
		candidates = append(candidates, minInt(base*4, 256))
	}
	out := make([]int, 0, len(candidates))
	seen := map[int]bool{}
	for _, v := range candidates {
		if v <= 0 || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func probeDelayCandidates() []time.Duration {
	return []time.Duration{
		0,
		10 * time.Microsecond,
		50 * time.Microsecond,
		200 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
	}
}

func minimumBufferedBudget(payloadBytes int64) int64 {
	floor := roundToMiB(maxInt64(1*MiB, payloadBytes*128))
	if floor < 1*MiB {
		return 1 * MiB
	}
	return floor
}

func initialBufferedBudget(totalMem uint64, payloadBytes int64) int64 {
	floor := maxInt64(32*MiB, roundToMiB(maxInt64(payloadBytes*4096, 0)))
	if totalMem == 0 {
		return floor
	}
	capBytes := roundToMiB(int64(totalMem / 16))
	if capBytes < floor {
		return floor
	}
	if capBytes > 256*MiB {
		capBytes = 256 * MiB
	}
	return capBytes
}

func maxBufferedBudget(totalMem uint64, payloadBytes int64) int64 {
	floor := maxInt64(64*MiB, roundToMiB(maxInt64(payloadBytes*8192, 0)))
	if totalMem == 0 {
		return floor
	}
	capBytes := roundToMiB(int64(totalMem / 8))
	if capBytes < floor {
		capBytes = floor
	}
	if capBytes > 1*GiB {
		capBytes = 1 * GiB
	}
	return capBytes
}

func probeBatchBytes(payloadBytes, bufferedBytes int64) int64 {
	if payloadBytes > 256*KiB {
		return 0
	}
	batch := recommendBatchBytes(payloadBytes, bufferedBytes)
	if batch <= 0 {
		return 0
	}
	return batch
}

func recommendBatchBytes(payloadBytes, bufferedBytes int64) int64 {
	if payloadBytes <= 0 || bufferedBytes <= 0 {
		return 0
	}
	if payloadBytes > 256*KiB {
		return 0
	}

	targetItems := int64(32)
	switch {
	case payloadBytes <= 1*KiB:
		targetItems = 128
	case payloadBytes <= 16*KiB:
		targetItems = 32
	case payloadBytes <= 128*KiB:
		targetItems = 8
	default:
		targetItems = 4
	}

	batch := payloadBytes * targetItems
	if batch < 32*KiB {
		batch = 32 * KiB
	}
	if batch > 4*MiB {
		batch = 4 * MiB
	}
	if batch > bufferedBytes/4 {
		batch = bufferedBytes / 4
	}
	if batch < payloadBytes*2 {
		batch = payloadBytes * 2
	}
	if batch <= 0 {
		return 0
	}
	return roundBatchSize(batch)
}

func roundBatchSize(v int64) int64 {
	switch {
	case v >= MiB:
		return ((v + MiB - 1) / MiB) * MiB
	case v >= KiB:
		return ((v + KiB - 1) / KiB) * KiB
	default:
		return v
	}
}

func roundToMiB(v int64) int64 {
	if v <= MiB {
		return v
	}
	return ((v + MiB - 1) / MiB) * MiB
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func detectTotalMemoryBytes() (uint64, error) {
	if raw := os.Getenv("TEMPO_TUNE_TOTAL_MEMORY_BYTES"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse TEMPO_TUNE_TOTAL_MEMORY_BYTES: %w", err)
		}
		return parsed, nil
	}

	switch runtime.GOOS {
	case "linux":
		return readMeminfoValue("MemTotal:")
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0, err
		}
		parsed, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported GOOS for memory detection: %s", runtime.GOOS)
	}
}

func readMeminfoValue(prefix string) (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return value * 1024, nil
	}
	return 0, fmt.Errorf("could not find %s in /proc/meminfo", prefix)
}

func formatBytes(v int64) string {
	switch {
	case v >= GiB:
		return fmt.Sprintf("%.2fGiB", float64(v)/float64(GiB))
	case v >= MiB:
		return fmt.Sprintf("%.2fMiB", float64(v)/float64(MiB))
	case v >= KiB:
		return fmt.Sprintf("%.2fKiB", float64(v)/float64(KiB))
	default:
		return fmt.Sprintf("%dB", v)
	}
}
