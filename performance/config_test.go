package performance

import (
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	tempo "github.com/ef2k/tempo"
)

func TestWriteTunedSettingsShapeOmitsNoise(t *testing.T) {
	settings := persistedTunedSettings{
		SoakDefaults: persistedSoakDefaults{
			Interval:         (10 * time.Millisecond).String(),
			MaxBatchBytes:    320 * tempo.KiB,
			MaxBufferedBytes: 4 * tempo.MiB,
			ConsumerDelay:    "0s",
		},
		TuneDefaults: persistedTuneDefaults{
			PayloadBytes: 10 * tempo.KiB,
		},
	}

	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatalf("marshal tuned settings: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode tuned settings: %v", err)
	}

	if _, ok := decoded["benchmark_defaults"]; ok {
		t.Fatal("unexpected benchmark_defaults in persisted tuned settings")
	}
	if _, ok := decoded["stress_defaults"]; ok {
		t.Fatal("unexpected stress_defaults in persisted tuned settings")
	}

	soak, ok := decoded["soak_defaults"].(map[string]any)
	if !ok {
		t.Fatal("missing soak_defaults in persisted tuned settings")
	}
	if got := soak["consumer_delay"]; got != "0s" {
		t.Fatalf("consumer_delay = %#v, want %q", got, "0s")
	}
}

func TestSoakDefaultConsumerDelayAllowsExplicitZero(t *testing.T) {
	path, err := settingsPath()
	if err != nil {
		t.Fatalf("settings path: %v", err)
	}
	original, readErr := os.ReadFile(path)
	hadOriginal := readErr == nil

	t.Cleanup(func() {
		settingsOnce = sync.Once{}
		settings = performanceSettings{}
		settingsErr = nil
		if hadOriginal {
			if err := os.WriteFile(path, original, 0644); err != nil {
				t.Fatalf("restore settings.json: %v", err)
			}
			return
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove settings.json: %v", err)
		}
	})

	settingsOnce = sync.Once{}
	settings = performanceSettings{}
	settingsErr = nil

	data := []byte("{\n  \"soak_defaults\": {\n    \"consumer_delay\": \"0s\"\n  }\n}\n")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write settings.json: %v", err)
	}

	delay := SoakDefaultConsumerDelay()
	if delay != 0 {
		t.Fatalf("delay = %s, want 0s", delay)
	}
}
