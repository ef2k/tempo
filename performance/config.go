package performance

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	tempo "github.com/ef2k/tempo"
)

const benchmarkMaxPendingBytes = 1 * tempo.GiB
const syntheticBenchPayloadBytes int64 = 7

type performanceSettings struct {
	MachineBaseline   machineBaselineSettings   `json:"machine_baseline"`
	BenchmarkDefaults benchmarkDefaultsSettings `json:"benchmark_defaults"`
	StressDefaults    stressDefaultsSettings    `json:"stress_defaults"`
	SoakDefaults      soakDefaultsSettings      `json:"soak_defaults"`
	TuneDefaults      tuneDefaultsSettings      `json:"tune_defaults"`
}

type machineBaselineSettings struct {
	Name        string `json:"name"`
	CPU         string `json:"cpu"`
	MemoryBytes int64  `json:"memory_bytes"`
	OS          string `json:"os"`
}

type benchmarkDefaultsSettings struct {
	IntervalSweep       []string `json:"interval_sweep"`
	ProducerParallelism []int    `json:"producer_parallelism"`
	SlowConsumerSweep   []string `json:"slow_consumer_sweep"`
}

type stressDefaultsSettings struct {
	MaxBatchBytes    int64  `json:"max_batch_bytes"`
	MaxPendingBytes  int64  `json:"max_pending_bytes"`
	Interval         string `json:"interval"`
	NumProducers     int    `json:"num_producers"`
	ItemsPerProducer int    `json:"items_per_producer"`
}

type soakDefaultsSettings struct {
	Interval        string `json:"interval"`
	MaxBatchBytes   int64  `json:"max_batch_bytes"`
	MaxPendingBytes int64  `json:"max_pending_bytes"`
	ConsumerDelay   string `json:"consumer_delay"`
	NumProducers    int    `json:"num_producers"`
	Duration        string `json:"duration"`
}

type tuneDefaultsSettings struct {
	Duration       string   `json:"duration"`
	ConsumerDelay  string   `json:"consumer_delay"`
	ConsumerDelays []string `json:"consumer_delays"`
	BatchBytes     []int64  `json:"batch_bytes"`
	PendingBytes   []int64  `json:"pending_bytes"`
}

type machineBaseline struct {
	Name        string
	CPU         string
	MemoryBytes int64
	OS          string
}

var (
	settingsOnce sync.Once
	settings     performanceSettings
	settingsErr  error
)

func settingsPath() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve performance settings path")
	}
	return filepath.Join(filepath.Dir(filename), "settings.json"), nil
}

func loadPerformanceSettings() (performanceSettings, error) {
	settingsOnce.Do(func() {
		settings = defaultPerformanceSettings()

		path, err := settingsPath()
		if err != nil {
			settingsErr = err
			return
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			settingsErr = err
			return
		}

		loaded := settings
		if err := json.Unmarshal(data, &loaded); err != nil {
			settingsErr = err
			return
		}
		settings = loaded
	})

	return settings, settingsErr
}

func PerformanceSettingsPath() (string, error) {
	return settingsPath()
}

func WriteTunedSettings(delay time.Duration, cfg tempo.Config) (string, error) {
	settings, err := loadPerformanceSettings()
	if err != nil {
		return "", err
	}
	path, err := settingsPath()
	if err != nil {
		return "", err
	}

	settings.SoakDefaults.ConsumerDelay = delay.String()
	settings.SoakDefaults.MaxBatchBytes = cfg.MaxBatchBytes
	settings.SoakDefaults.MaxPendingBytes = cfg.MaxPendingBytes
	settings.SoakDefaults.Interval = cfg.Interval.String()
	settings.TuneDefaults.ConsumerDelay = delay.String()

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}

	settingsOnce = sync.Once{}
	settingsErr = nil
	return path, nil
}

func defaultPerformanceSettings() performanceSettings {
	return performanceSettings{
		MachineBaseline: machineBaselineSettings{
			Name:        "apple-m4-16gb",
			CPU:         "Apple M4",
			MemoryBytes: 16 * tempo.GiB,
			OS:          "macOS",
		},
		BenchmarkDefaults: benchmarkDefaultsSettings{
			IntervalSweep:       []string{"100us", "1ms", "10ms", "100ms"},
			ProducerParallelism: []int{1, 4, 16, 64, 256},
			SlowConsumerSweep:   []string{"10us", "100us", "1ms"},
		},
		StressDefaults: stressDefaultsSettings{
			MaxBatchBytes:    256 * syntheticBenchPayloadBytes,
			MaxPendingBytes:  1 * tempo.GiB,
			Interval:         time.Hour.String(),
			NumProducers:     256,
			ItemsPerProducer: 2000,
		},
		SoakDefaults: soakDefaultsSettings{
			Interval:        (10 * time.Millisecond).String(),
			MaxBatchBytes:   32 * tempo.KiB,
			MaxPendingBytes: 64 * tempo.MiB,
			ConsumerDelay:   (200 * time.Microsecond).String(),
			NumProducers:    32,
			Duration:        (5 * time.Minute).String(),
		},
		TuneDefaults: tuneDefaultsSettings{
			Duration:       (15 * time.Second).String(),
			ConsumerDelay:  (200 * time.Microsecond).String(),
			ConsumerDelays: []string{"100us", "150us", "200us", "300us", "400us"},
			BatchBytes:     []int64{4 * tempo.KiB, 7 * tempo.KiB, 8 * tempo.KiB, 16 * tempo.KiB, 32 * tempo.KiB},
			PendingBytes:   []int64{64 * tempo.MiB, 128 * tempo.MiB, 256 * tempo.MiB, 512 * tempo.MiB},
		},
	}
}

func TunedMachineBaseline() machineBaseline {
	cfg, _ := loadPerformanceSettings()
	return machineBaseline{
		Name:        cfg.MachineBaseline.Name,
		CPU:         cfg.MachineBaseline.CPU,
		MemoryBytes: cfg.MachineBaseline.MemoryBytes,
		OS:          cfg.MachineBaseline.OS,
	}
}

func benchmarkConfig(maxBatchBytes int64) *tempo.Config {
	return benchmarkConfigWithInterval(maxBatchBytes, time.Hour)
}

func benchmarkConfigWithInterval(maxBatchBytes int64, interval time.Duration) *tempo.Config {
	return &tempo.Config{
		Interval:        interval,
		MaxBatchBytes:   maxBatchBytes,
		MaxPendingBytes: benchmarkMaxPendingBytes,
	}
}

func SoakDefaultConfig() tempo.Config {
	cfg, _ := loadPerformanceSettings()
	interval, _ := time.ParseDuration(cfg.SoakDefaults.Interval)
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	return tempo.Config{
		Interval:        interval,
		MaxBatchBytes:   cfg.SoakDefaults.MaxBatchBytes,
		MaxPendingBytes: cfg.SoakDefaults.MaxPendingBytes,
	}
}

func SoakConfigFor(maxBatchBytes, maxPendingBytes int64) tempo.Config {
	cfg := SoakDefaultConfig()
	cfg.MaxBatchBytes = maxBatchBytes
	cfg.MaxPendingBytes = maxPendingBytes
	return cfg
}

func SoakDefaultConsumerDelay() time.Duration {
	cfg, _ := loadPerformanceSettings()
	delay, _ := time.ParseDuration(cfg.SoakDefaults.ConsumerDelay)
	if delay <= 0 {
		return 200 * time.Microsecond
	}
	return delay
}

func SoakDefaultNumProducers() int {
	cfg, _ := loadPerformanceSettings()
	if cfg.SoakDefaults.NumProducers <= 0 {
		return 32
	}
	return cfg.SoakDefaults.NumProducers
}

func SoakDefaultDuration() time.Duration {
	cfg, _ := loadPerformanceSettings()
	duration, _ := time.ParseDuration(cfg.SoakDefaults.Duration)
	if duration <= 0 {
		return 5 * time.Minute
	}
	return duration
}

func TuneDefaultDuration() time.Duration {
	cfg, _ := loadPerformanceSettings()
	duration, _ := time.ParseDuration(cfg.TuneDefaults.Duration)
	if duration <= 0 {
		return 15 * time.Second
	}
	return duration
}

func TuneDefaultConsumerDelay() time.Duration {
	cfg, _ := loadPerformanceSettings()
	delay, _ := time.ParseDuration(cfg.TuneDefaults.ConsumerDelay)
	if delay <= 0 {
		return 200 * time.Microsecond
	}
	return delay
}

func TuneDefaultConsumerDelayCandidates() []time.Duration {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.TuneDefaults.ConsumerDelays) == 0 {
		return []time.Duration{100 * time.Microsecond, 150 * time.Microsecond, 200 * time.Microsecond, 300 * time.Microsecond, 400 * time.Microsecond}
	}
	out := make([]time.Duration, 0, len(cfg.TuneDefaults.ConsumerDelays))
	for _, raw := range cfg.TuneDefaults.ConsumerDelays {
		parsed, err := time.ParseDuration(raw)
		if err == nil && parsed > 0 {
			out = append(out, parsed)
		}
	}
	if len(out) == 0 {
		return []time.Duration{100 * time.Microsecond, 150 * time.Microsecond, 200 * time.Microsecond, 300 * time.Microsecond, 400 * time.Microsecond}
	}
	return out
}

func TuneDefaultBatchCandidates() []int64 {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.TuneDefaults.BatchBytes) == 0 {
		return []int64{4 * tempo.KiB, 7 * tempo.KiB, 8 * tempo.KiB, 16 * tempo.KiB, 32 * tempo.KiB}
	}
	return append([]int64(nil), cfg.TuneDefaults.BatchBytes...)
}

func TuneDefaultPendingCandidates() []int64 {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.TuneDefaults.PendingBytes) == 0 {
		return []int64{64 * tempo.MiB, 128 * tempo.MiB, 256 * tempo.MiB, 512 * tempo.MiB}
	}
	return append([]int64(nil), cfg.TuneDefaults.PendingBytes...)
}

func BenchmarkDefaultIntervalSweep() []time.Duration {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.BenchmarkDefaults.IntervalSweep) == 0 {
		return []time.Duration{100 * time.Microsecond, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond}
	}
	out := make([]time.Duration, 0, len(cfg.BenchmarkDefaults.IntervalSweep))
	for _, raw := range cfg.BenchmarkDefaults.IntervalSweep {
		parsed, err := time.ParseDuration(raw)
		if err == nil && parsed > 0 {
			out = append(out, parsed)
		}
	}
	if len(out) == 0 {
		return []time.Duration{100 * time.Microsecond, time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond}
	}
	return out
}

func BenchmarkDefaultProducerParallelism() []int {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.BenchmarkDefaults.ProducerParallelism) == 0 {
		return []int{1, 4, 16, 64, 256}
	}
	return append([]int(nil), cfg.BenchmarkDefaults.ProducerParallelism...)
}

func BenchmarkDefaultSlowConsumerSweep() []time.Duration {
	cfg, _ := loadPerformanceSettings()
	if len(cfg.BenchmarkDefaults.SlowConsumerSweep) == 0 {
		return []time.Duration{10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond}
	}
	out := make([]time.Duration, 0, len(cfg.BenchmarkDefaults.SlowConsumerSweep))
	for _, raw := range cfg.BenchmarkDefaults.SlowConsumerSweep {
		parsed, err := time.ParseDuration(raw)
		if err == nil && parsed > 0 {
			out = append(out, parsed)
		}
	}
	if len(out) == 0 {
		return []time.Duration{10 * time.Microsecond, 100 * time.Microsecond, time.Millisecond}
	}
	return out
}

func StressDefaultConfig() tempo.Config {
	cfg, _ := loadPerformanceSettings()
	interval, _ := time.ParseDuration(cfg.StressDefaults.Interval)
	if interval <= 0 {
		interval = time.Hour
	}
	maxBatchBytes := cfg.StressDefaults.MaxBatchBytes
	if maxBatchBytes <= 0 {
		maxBatchBytes = 256 * syntheticBenchPayloadBytes
	}
	maxPendingBytes := cfg.StressDefaults.MaxPendingBytes
	if maxPendingBytes <= 0 {
		maxPendingBytes = 1 * tempo.GiB
	}
	return tempo.Config{
		Interval:        interval,
		MaxBatchBytes:   maxBatchBytes,
		MaxPendingBytes: maxPendingBytes,
	}
}

func StressDefaultNumProducers() int {
	cfg, _ := loadPerformanceSettings()
	if cfg.StressDefaults.NumProducers <= 0 {
		return 256
	}
	return cfg.StressDefaults.NumProducers
}

func StressDefaultItemsPerProducer() int {
	cfg, _ := loadPerformanceSettings()
	if cfg.StressDefaults.ItemsPerProducer <= 0 {
		return 2000
	}
	return cfg.StressDefaults.ItemsPerProducer
}
