package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ef2k/tempo/performance"
	"github.com/ef2k/tempo/performance/tuner"
)

func main() {
	duration := flag.Duration("duration", performance.TuneDefaultDuration(), "probe duration")
	payloadArg := flag.String("payload-bytes", "", "approximate payload size (examples: 512B, 4KiB, 64KiB, 1MiB)")
	flag.Parse()

	payloadBytes, err := resolvePayloadBytes(*payloadArg)
	if err != nil {
		exitf("resolve payload size: %v", err)
	}

	fmt.Printf("tempo tune\n")
	fmt.Printf("  probe duration: %s\n", duration.String())
	fmt.Printf("  payload size:   %s\n", formatBytes(payloadBytes))

	recommendation, err := tuner.Tune(context.Background(), tuner.Options{
		PayloadBytes:  payloadBytes,
		ProbeDuration: *duration,
	})
	if err != nil {
		exitf("tune tempo: %v", err)
	}

	if recommendation.TotalMemoryBytes > 0 {
		fmt.Printf("  detected total memory: %s\n", formatBytes(int64(recommendation.TotalMemoryBytes)))
	}

	fmt.Println()
	fmt.Printf("observed\n")
	fmt.Printf("  producers:             %d\n", recommendation.ProducerCount)
	fmt.Printf("  observed items/sec:    %.0f\n", recommendation.ObservedItemsPerSec)
	fmt.Printf("  observed bytes/sec:    %s/sec\n", formatBytes(int64(recommendation.ObservedBytesPerSec)))
	fmt.Printf("  peak heap alloc:       %s\n", formatBytes(int64(recommendation.ObservedPeakHeapBytes)))
	fmt.Printf("  peak buffered bytes:   %s\n", formatBytes(recommendation.ObservedPeakBuffered))
	fmt.Printf("  queue-full rejections: %d\n", recommendation.Rejections)

	fmt.Println()
	fmt.Printf("recommended\n")
	fmt.Printf("  MaxBufferedBytes: %d // %s\n", recommendation.RecommendedMaxBuffered, formatBytes(recommendation.RecommendedMaxBuffered))
	if recommendation.BatchShapingRecommended {
		fmt.Printf("  MaxBatchBytes:    %d // %s\n", recommendation.RecommendedMaxBatch, formatBytes(recommendation.RecommendedMaxBatch))
	} else {
		fmt.Printf("  MaxBatchBytes:    0 // leave batch shaping disabled initially\n")
	}
	fmt.Printf("  buffered items:   ~%d payloads\n", recommendation.BufferedItemCapacity)
	if recommendation.EstimatedBurstWindow > 0 {
		fmt.Printf("  burst headroom:   ~%s at observed top speed\n", recommendation.EstimatedBurstWindow.Round(100*time.Millisecond))
	}

	if len(recommendation.Notes) > 0 {
		fmt.Println()
		fmt.Printf("notes\n")
		for _, note := range recommendation.Notes {
			fmt.Printf("  - %s\n", note)
		}
	}

	maybeWriteSettings(payloadBytes, recommendation)
}

func resolvePayloadBytes(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw != "" {
		return parseByteSize(raw)
	}

	if isInteractiveTerminal() {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("\napproximate size of one payload [default %s]: ", formatBytes(performance.TuneDefaultPayloadBytes()))
		answer, err := reader.ReadString('\n')
		if err != nil {
			return 0, err
		}
		answer = strings.TrimSpace(answer)
		if answer == "" {
			return performance.TuneDefaultPayloadBytes(), nil
		}
		return parseByteSize(answer)
	}

	return performance.TuneDefaultPayloadBytes(), nil
}

func parseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(strings.ToUpper(raw))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}

	multiplier := int64(1)
	for _, suffix := range []struct {
		Suffix string
		Scale  int64
	}{
		{"GIB", tuner.GiB},
		{"GB", 1000 * 1000 * 1000},
		{"MIB", tuner.MiB},
		{"MB", 1000 * 1000},
		{"KIB", tuner.KiB},
		{"KB", 1000},
		{"B", 1},
	} {
		if strings.HasSuffix(s, suffix.Suffix) {
			s = strings.TrimSpace(strings.TrimSuffix(s, suffix.Suffix))
			multiplier = suffix.Scale
			break
		}
	}

	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("size must be greater than zero")
	}
	return int64(value * float64(multiplier)), nil
}

func maybeWriteSettings(payloadBytes int64, recommendation tuner.Recommendation) {
	if !isInteractiveTerminal() {
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\nwrite recommended settings to performance/settings.json? [y/N]: ")
	answer, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			fmt.Println("leaving performance/settings.json unchanged")
			return
		}
		fmt.Fprintf(os.Stderr, "read answer: %v\n", err)
		return
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	if answer != "y" && answer != "yes" {
		fmt.Println("leaving performance/settings.json unchanged")
		return
	}

	path, err := performance.WriteTunedSettings(payloadBytes, performance.SoakConfigFor(recommendation.RecommendedMaxBatch, recommendation.RecommendedMaxBuffered))
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

func exitf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
