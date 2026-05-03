package tuner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	KiB int64 = 1024
	MiB       = 1024 * KiB
	GiB       = 1024 * MiB
)

type Options struct {
	RepoRoot       string
	SoakDuration   time.Duration
	ConsumerDelay  time.Duration
	ConsumerDelays []time.Duration
	TestTimeout    time.Duration
	BatchBytes     []int64
	PendingBytes   []int64
}

type Candidate struct {
	ConsumerDelay   time.Duration
	MaxBatchBytes   int64
	MaxPendingBytes int64
}

type Recommendation struct {
	TotalMemoryBytes uint64
	Candidates       []Candidate
	Runs             []RunResult
	Best             RunResult
}

type RunResult struct {
	Candidate               Candidate
	Produced                int64
	Rejected                int64
	Delivered               int64
	AvgDeliveredItemsPerSec float64
	PeakHeapAllocBytes      uint64
	OverallStatus           string
	ResultsPath             string
	StreamPath              string
}

type soakSnapshot struct {
	Produced                int64   `json:"produced"`
	Rejected                int64   `json:"rejected"`
	Delivered               int64   `json:"delivered"`
	AvgDeliveredItemsPerSec float64 `json:"avg_delivered_items_per_second"`
	PeakHeapAllocBytes      uint64  `json:"peak_heap_alloc_bytes"`
	Assessment              struct {
		Overall struct {
			Status string `json:"status"`
		} `json:"overall"`
	} `json:"assessment"`
}

func Tune(ctx context.Context, opts Options) (Recommendation, error) {
	if opts.RepoRoot == "" {
		return Recommendation{}, errors.New("repo root is required")
	}
	if opts.SoakDuration <= 0 {
		opts.SoakDuration = 15 * time.Second
	}
	if opts.TestTimeout <= 0 {
		opts.TestTimeout = 3 * time.Minute
	}

	totalMem, _ := detectTotalMemoryBytes()
	if len(opts.BatchBytes) == 0 {
		opts.BatchBytes = []int64{4 * KiB, 7 * KiB, 8 * KiB, 16 * KiB, 32 * KiB}
	}
	if len(opts.PendingBytes) == 0 {
		opts.PendingBytes = defaultPendingCandidates(totalMem)
	}
	if opts.ConsumerDelay > 0 {
		opts.ConsumerDelays = []time.Duration{opts.ConsumerDelay}
	}
	if len(opts.ConsumerDelays) == 0 {
		opts.ConsumerDelays = []time.Duration{200 * time.Microsecond}
	}

	opts.BatchBytes = uniquePositive(opts.BatchBytes)
	opts.PendingBytes = uniquePositive(opts.PendingBytes)
	opts.ConsumerDelays = uniqueDurations(opts.ConsumerDelays)

	if len(opts.BatchBytes) == 0 || len(opts.PendingBytes) == 0 || len(opts.ConsumerDelays) == 0 {
		return Recommendation{}, errors.New("no candidate values to test")
	}

	candidates := make([]Candidate, 0, len(opts.ConsumerDelays)*len(opts.BatchBytes)*len(opts.PendingBytes))
	for _, delay := range opts.ConsumerDelays {
		for _, batch := range opts.BatchBytes {
			for _, pending := range opts.PendingBytes {
				if pending < batch {
					continue
				}
				candidates = append(candidates, Candidate{
					ConsumerDelay:   delay,
					MaxBatchBytes:   batch,
					MaxPendingBytes: pending,
				})
			}
		}
	}

	if len(candidates) == 0 {
		return Recommendation{}, errors.New("no valid candidate combinations")
	}

	runs := make([]RunResult, 0, len(candidates))
	for _, candidate := range candidates {
		run, err := runSoak(ctx, opts, candidate)
		if err != nil {
			return Recommendation{}, err
		}
		runs = append(runs, run)
	}

	best, err := chooseBest(runs)
	if err != nil {
		return Recommendation{}, err
	}

	return Recommendation{
		TotalMemoryBytes: totalMem,
		Candidates:       candidates,
		Runs:             runs,
		Best:             best,
	}, nil
}

func runSoak(ctx context.Context, opts Options, candidate Candidate) (RunResult, error) {
	outDir, err := os.MkdirTemp("", "tempo-tune-*")
	if err != nil {
		return RunResult{}, err
	}
	defer os.RemoveAll(outDir)

	cmd := exec.CommandContext(
		ctx,
		"go", "test",
		"-timeout", opts.TestTimeout.String(),
		"-run", "^TestSoakSustainedLoadStaysHealthy$",
		"-v", "./performance",
	)
	cmd.Dir = opts.RepoRoot
	cmd.Env = append(os.Environ(),
		"TEMPO_RUN_SOAK=1",
		"TEMPO_SOAK_DURATION="+opts.SoakDuration.String(),
		"TEMPO_SOAK_CONSUMER_DELAY="+candidate.ConsumerDelay.String(),
		"TEMPO_SOAK_MAX_BATCH_BYTES="+strconv.FormatInt(candidate.MaxBatchBytes, 10),
		"TEMPO_SOAK_MAX_PENDING_BYTES="+strconv.FormatInt(candidate.MaxPendingBytes, 10),
		"TEMPO_SOAK_OUT_DIR="+outDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return RunResult{}, fmt.Errorf("run soak for delay=%s batch=%d pending=%d: %w\n%s", candidate.ConsumerDelay, candidate.MaxBatchBytes, candidate.MaxPendingBytes, err, output)
	}

	matches, err := filepath.Glob(filepath.Join(outDir, "*-results.json"))
	if err != nil {
		return RunResult{}, err
	}
	if len(matches) != 1 {
		return RunResult{}, fmt.Errorf("expected one results file in %s, found %d", outDir, len(matches))
	}

	snapshotBytes, err := os.ReadFile(matches[0])
	if err != nil {
		return RunResult{}, err
	}

	var snapshot soakSnapshot
	if err := json.Unmarshal(snapshotBytes, &snapshot); err != nil {
		return RunResult{}, err
	}

	streamPath := strings.TrimSuffix(matches[0], "-results.json") + ".jsonl"
	return RunResult{
		Candidate:               candidate,
		Produced:                snapshot.Produced,
		Rejected:                snapshot.Rejected,
		Delivered:               snapshot.Delivered,
		AvgDeliveredItemsPerSec: snapshot.AvgDeliveredItemsPerSec,
		PeakHeapAllocBytes:      snapshot.PeakHeapAllocBytes,
		OverallStatus:           snapshot.Assessment.Overall.Status,
		ResultsPath:             matches[0],
		StreamPath:              streamPath,
	}, nil
}

func chooseBest(runs []RunResult) (RunResult, error) {
	if len(runs) == 0 {
		return RunResult{}, errors.New("no tuning runs recorded")
	}

	best := runs[0]
	for _, run := range runs[1:] {
		if better(run, best) {
			best = run
		}
	}
	return best, nil
}

func better(a, b RunResult) bool {
	aPass := a.OverallStatus == "pass"
	bPass := b.OverallStatus == "pass"
	if aPass != bPass {
		return aPass
	}
	if a.AvgDeliveredItemsPerSec != b.AvgDeliveredItemsPerSec {
		return a.AvgDeliveredItemsPerSec > b.AvgDeliveredItemsPerSec
	}
	if a.Delivered != b.Delivered {
		return a.Delivered > b.Delivered
	}
	if a.Rejected != b.Rejected {
		return a.Rejected < b.Rejected
	}
	return a.PeakHeapAllocBytes < b.PeakHeapAllocBytes
}

func defaultPendingCandidates(totalMem uint64) []int64 {
	if totalMem == 0 {
		return []int64{64 * MiB, 128 * MiB, 256 * MiB, 512 * MiB}
	}

	divisors := []uint64{256, 128, 64, 32}
	out := make([]int64, 0, len(divisors))
	for _, divisor := range divisors {
		candidate := int64(totalMem / divisor)
		if candidate < 64*MiB {
			candidate = 64 * MiB
		}
		if candidate > 1*GiB {
			candidate = 1 * GiB
		}
		candidate = roundToMiB(candidate)
		out = append(out, candidate)
	}
	return uniquePositive(out)
}

func uniquePositive(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func uniqueDurations(values []time.Duration) []time.Duration {
	seen := make(map[time.Duration]struct{}, len(values))
	out := make([]time.Duration, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)
	return out
}

func roundToMiB(v int64) int64 {
	if v <= MiB {
		return v
	}
	return (v / MiB) * MiB
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
		return 0, fmt.Errorf("unsupported platform for automatic memory detection: %s", runtime.GOOS)
	}
}

func readMeminfoValue(key string) (uint64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, key) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("unexpected meminfo line for %s", key)
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return value * 1024, nil
	}
	return 0, fmt.Errorf("meminfo key %s not found", key)
}
