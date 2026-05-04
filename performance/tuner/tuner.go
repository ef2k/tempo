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
)

type Options struct {
	PayloadBytes  int64
	ProbeDuration time.Duration
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
	RecommendedMaxBuffered  int64
	RecommendedMaxBatch     int64
	BufferedItemCapacity    int64
	EstimatedBurstWindow    time.Duration
	Rejections              int64
	BatchShapingRecommended bool
	Notes                   []string
}

func Tune(ctx context.Context, opts Options) (Recommendation, error) {
	if opts.PayloadBytes <= 0 {
		return Recommendation{}, errors.New("payload bytes must be greater than zero")
	}
	if opts.ProbeDuration <= 0 {
		opts.ProbeDuration = 5 * time.Second
	}

	totalMem, _ := detectTotalMemoryBytes()
	producers := recommendedProducerCount()
	probeBuffered := probeBufferedBudget(totalMem, opts.PayloadBytes)
	probeBatch := probeBatchBytes(opts.PayloadBytes, probeBuffered)

	itemsPerSec, bytesPerSec, peakHeap, peakBuffered, rejected, err := runProbe(ctx, probeConfig{
		PayloadBytes:     opts.PayloadBytes,
		ProbeDuration:    opts.ProbeDuration,
		ProducerCount:    producers,
		MaxBufferedBytes: probeBuffered,
		MaxBatchBytes:    probeBatch,
	})
	if err != nil {
		return Recommendation{}, err
	}

	recommendedBuffered := recommendBufferedBudget(totalMem, opts.PayloadBytes, bytesPerSec)
	recommendedBatch := recommendBatchBytes(opts.PayloadBytes, recommendedBuffered)
	batchRecommended := recommendedBatch > 0
	bufferedItems := int64(0)
	if opts.PayloadBytes > 0 {
		bufferedItems = recommendedBuffered / opts.PayloadBytes
	}

	burstWindow := time.Duration(0)
	if bytesPerSec > 0 {
		seconds := float64(recommendedBuffered) / bytesPerSec
		burstWindow = time.Duration(seconds * float64(time.Second))
	}

	notes := []string{
		"MaxBufferedBytes is the primary safety boundary.",
		"MaxBatchBytes is optional shaping; leave it unset if downstream batch size does not matter.",
	}
	if !batchRecommended {
		notes = append(notes, "Payloads are large enough that batch-size shaping is not recommended by default on this machine.")
	} else {
		notes = append(notes, fmt.Sprintf("Recommended MaxBatchBytes is sized to fit several payloads without becoming brittle for single large items."))
	}
	if totalMem > 0 {
		notes = append(notes, fmt.Sprintf("Recommendation is capped to a conservative fraction of detected machine memory (%s total).", formatBytes(int64(totalMem))))
	}
	if rejected > 0 {
		notes = append(notes, "The probe observed some queue pressure while measuring top speed; consider a larger machine if your real traffic is burstier than this payload model.")
	}

	return Recommendation{
		TotalMemoryBytes:        totalMem,
		PayloadBytes:            opts.PayloadBytes,
		ProbeDuration:           opts.ProbeDuration,
		ProducerCount:           producers,
		ObservedItemsPerSec:     itemsPerSec,
		ObservedBytesPerSec:     bytesPerSec,
		ObservedPeakHeapBytes:   peakHeap,
		ObservedPeakBuffered:    peakBuffered,
		RecommendedMaxBuffered:  recommendedBuffered,
		RecommendedMaxBatch:     recommendedBatch,
		BufferedItemCapacity:    bufferedItems,
		EstimatedBurstWindow:    burstWindow,
		Rejections:              rejected,
		BatchShapingRecommended: batchRecommended,
		Notes:                   notes,
	}, nil
}

type probeConfig struct {
	PayloadBytes     int64
	ProbeDuration    time.Duration
	ProducerCount    int
	MaxBufferedBytes int64
	MaxBatchBytes    int64
}

func runProbe(ctx context.Context, cfg probeConfig) (itemsPerSec, bytesPerSec float64, peakHeapBytes uint64, peakBufferedBytes int64, rejected int64, err error) {
	d, err := tempo.NewDispatcher(&tempo.Config{
		Interval:         5 * time.Millisecond,
		MaxBatchBytes:    cfg.MaxBatchBytes,
		MaxBufferedBytes: cfg.MaxBufferedBytes,
	})
	if err != nil {
		return 0, 0, 0, 0, 0, err
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), max(2*time.Second, cfg.ProbeDuration))
	defer cancel()
	if shutdownErr := d.Shutdown(shutdownCtx); err == nil && shutdownErr != nil {
		err = shutdownErr
	}
	close(stopConsumers)
	consumerWG.Wait()
	samplerWG.Wait()

	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	durationSeconds := cfg.ProbeDuration.Seconds()
	if durationSeconds <= 0 {
		durationSeconds = 1
	}
	itemsPerSec = float64(delivered.Load()) / durationSeconds
	bytesPerSec = itemsPerSec * float64(cfg.PayloadBytes)

	return itemsPerSec, bytesPerSec, peakHeap, peakBuffered.Load() * cfg.PayloadBytes, rejectedCount.Load(), nil
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

func probeBufferedBudget(totalMem uint64, payloadBytes int64) int64 {
	floor := maxInt64(16*MiB, roundToMiB(maxInt64(payloadBytes*2048, 0)))
	if totalMem == 0 {
		return floor
	}
	capBytes := roundToMiB(int64(totalMem / 8))
	if capBytes < floor {
		return floor
	}
	if capBytes > 512*MiB {
		capBytes = 512 * MiB
	}
	return capBytes
}

func recommendBufferedBudget(totalMem uint64, payloadBytes int64, bytesPerSec float64) int64 {
	floor := maxInt64(16*MiB, roundToMiB(maxInt64(payloadBytes*4096, 0)))
	target := int64(bytesPerSec * 3)
	if target < floor {
		target = floor
	}
	target = roundToMiB(target)

	if totalMem == 0 {
		return target
	}

	capBytes := roundToMiB(int64(totalMem / 8))
	if capBytes < floor {
		capBytes = floor
	}
	if capBytes > 1*GiB {
		capBytes = 1 * GiB
	}
	if target > capBytes {
		return capBytes
	}
	return target
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
