package tuner

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProbeProducerCandidatesAddHigherPressureOptions(t *testing.T) {
	got := probeProducerCandidates(40)
	want := []int{1, 2, 20, 40, 80, 160}

	if len(got) != len(want) {
		t.Fatalf("candidate count = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate %d = %d, want %d (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestProbeDelayCandidatesReachMillisecondBackpressure(t *testing.T) {
	got := probeDelayCandidates()
	want := []time.Duration{
		0,
		10 * time.Microsecond,
		50 * time.Microsecond,
		200 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
	}

	if len(got) != len(want) {
		t.Fatalf("delay count = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delay %d = %s, want %s (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestMinimumBufferedBudgetAllowsSmallPayloadPressure(t *testing.T) {
	got := minimumBufferedBudget(10 * KiB)
	want := int64(2 * MiB)
	if got != want {
		t.Fatalf("minimum budget = %d, want %d", got, want)
	}
}

func TestProbeRunHealthRequiresZeroRejections(t *testing.T) {
	cases := []struct {
		name   string
		result probeResult
		want   bool
	}{
		{
			name: "clean run is healthy",
			result: probeResult{
				rejections: 0,
			},
			want: true,
		},
		{
			name: "any rejection is pressure",
			result: probeResult{
				rejections:    1,
				rejectionRate: 0.0001,
			},
			want: false,
		},
		{
			name: "timeout is unhealthy",
			result: probeResult{
				rejections: 0,
				timedOut:   true,
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.result.rejections == 0 && !tc.result.timedOut
			if got != tc.want {
				t.Fatalf("healthy = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestRunBudgetedProbeStepStopsAtBudget(t *testing.T) {
	remaining := 0
	_, err := runBudgetedProbeStep(context.Background(), probeConfig{}, &remaining)
	if !errors.Is(err, errTuneProbeBudgetExhausted) {
		t.Fatalf("error = %v, want %v", err, errTuneProbeBudgetExhausted)
	}
}
