package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ef2k/tempo/performance"
	"github.com/ef2k/tempo/performance/tuner"
)

func main() {
	duration := flag.Duration("duration", performance.TuneDefaultDuration(), "soak duration per candidate")
	delay := flag.Duration("consumer-delay", performance.TuneDefaultConsumerDelay(), "consumer delay for the soak workload")
	timeout := flag.Duration("timeout", 3*time.Minute, "go test timeout per candidate run")
	batchArg := flag.String("batch-bytes", "", "comma-separated batch byte candidates")
	pendingArg := flag.String("pending-bytes", "", "comma-separated pending byte candidates")
	delayArg := flag.String("consumer-delays", "", "comma-separated consumer delay candidates")
	flag.Parse()
	explicitDelay := flagWasProvided("consumer-delay")

	repoRoot, err := os.Getwd()
	if err != nil {
		exitf("get working directory: %v", err)
	}
	repoRoot, err = filepath.Abs(repoRoot)
	if err != nil {
		exitf("resolve repo root: %v", err)
	}

	batches, err := parseInt64List(*batchArg)
	if err != nil {
		exitf("parse -batch-bytes: %v", err)
	}
	pending, err := parseInt64List(*pendingArg)
	if err != nil {
		exitf("parse -pending-bytes: %v", err)
	}
	delays, err := parseDurationList(*delayArg)
	if err != nil {
		exitf("parse -consumer-delays: %v", err)
	}
	effectiveDelays := defaultDurationsIfEmpty(delays, performance.TuneDefaultConsumerDelayCandidates())

	fmt.Printf("tempo tune\n")
	fmt.Printf("  repo: %s\n", repoRoot)
	fmt.Printf("  soak duration per candidate: %s\n", duration.String())
	if explicitDelay {
		fmt.Printf("  consumer delay: %s\n", delay.String())
	} else {
		fmt.Printf("  consumer delays: %s\n", formatDurationList(effectiveDelays))
	}

	recommendation, err := tuner.Tune(context.Background(), tuner.Options{
		RepoRoot:       repoRoot,
		SoakDuration:   *duration,
		ConsumerDelay:  consumerDelayOverride(*delay, delays, explicitDelay),
		ConsumerDelays: effectiveDelays,
		TestTimeout:    *timeout,
		BatchBytes:     defaultIfEmpty(batches, performance.TuneDefaultBatchCandidates()),
		PendingBytes:   defaultIfEmpty(pending, performance.TuneDefaultPendingCandidates()),
	})
	if err != nil {
		exitf("tune tempo: %v", err)
	}

	if recommendation.TotalMemoryBytes > 0 {
		fmt.Printf("  detected total memory: %s\n", formatBytes(int64(recommendation.TotalMemoryBytes)))
	}
	fmt.Println()
	fmt.Printf("%-10s %-12s %-14s %-12s %-12s %-10s %-10s\n", "delay", "batch", "pending", "delivered/s", "peak heap", "rejected", "status")
	for _, run := range recommendation.Runs {
		fmt.Printf(
			"%-10s %-12s %-14s %-12.0f %-12s %-10d %-10s\n",
			run.Candidate.ConsumerDelay.String(),
			formatBytes(run.Candidate.MaxBatchBytes),
			formatBytes(run.Candidate.MaxBufferedBytes),
			run.AvgDeliveredItemsPerSec,
			formatBytes(int64(run.PeakHeapAllocBytes)),
			run.Rejected,
			run.OverallStatus,
		)
	}

	best := recommendation.Best
	fmt.Println()
	fmt.Printf("recommended\n")
	fmt.Printf("  ConsumerDelay:  %s\n", best.Candidate.ConsumerDelay)
	fmt.Printf("  MaxBatchBytes:   %d // %s\n", best.Candidate.MaxBatchBytes, formatBytes(best.Candidate.MaxBatchBytes))
	fmt.Printf("  MaxBufferedBytes: %d // %s\n", best.Candidate.MaxBufferedBytes, formatBytes(best.Candidate.MaxBufferedBytes))
	fmt.Printf("  AvgDeliveredItemsPerSec: %.0f\n", best.AvgDeliveredItemsPerSec)
	fmt.Printf("  PeakHeapAlloc: %s\n", formatBytes(int64(best.PeakHeapAllocBytes)))
	fmt.Printf("  Rejected: %d\n", best.Rejected)
	fmt.Printf("  Overall: %s\n", best.OverallStatus)

	maybeWriteSettings(best)
}

func defaultIfEmpty(values, fallback []int64) []int64 {
	if len(values) > 0 {
		return values
	}
	return append([]int64(nil), fallback...)
}

func defaultDurationsIfEmpty(values, fallback []time.Duration) []time.Duration {
	if len(values) > 0 {
		return values
	}
	return append([]time.Duration(nil), fallback...)
}

func consumerDelayOverride(value time.Duration, delays []time.Duration, explicit bool) time.Duration {
	if len(delays) > 0 || !explicit {
		return 0
	}
	return value
}

func flagWasProvided(name string) bool {
	provided := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}

func parseInt64List(raw string) ([]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int64, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func parseDurationList(raw string) ([]time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		value, err := time.ParseDuration(part)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func formatDurationList(values []time.Duration) string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = value.String()
	}
	return strings.Join(out, ", ")
}

func formatBytes(v int64) string {
	switch {
	case v >= tuner.GiB:
		return fmt.Sprintf("%.2fGiB", float64(v)/float64(tuner.GiB))
	case v >= tuner.MiB:
		return fmt.Sprintf("%.2fMiB", float64(v)/float64(tuner.MiB))
	case v >= tuner.KiB:
		return fmt.Sprintf("%.2fKiB", float64(v)/float64(tuner.KiB))
	default:
		return fmt.Sprintf("%dB", v)
	}
}

func maybeWriteSettings(best tuner.RunResult) {
	if !isInteractiveTerminal() {
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nwrite recommended settings to performance/settings.json? [y/N]: ")
	answer, err := reader.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "read answer: %v\n", err)
		return
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	if answer != "y" && answer != "yes" {
		fmt.Println("leaving performance/settings.json unchanged")
		return
	}

	path, err := performance.WriteTunedSettings(best.Candidate.ConsumerDelay, performance.SoakConfigFor(best.Candidate.MaxBatchBytes, best.Candidate.MaxBufferedBytes))
	if err != nil {
		fmt.Fprintf(os.Stderr, "write settings: %v\n", err)
		return
	}
	fmt.Printf("wrote recommended settings to %s\n", path)
}

func isInteractiveTerminal() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
