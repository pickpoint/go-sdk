package tracking_test

import (
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestBackoffFullJitter(t *testing.T) {
	state := tracking.NewBackoff(100*time.Millisecond, 800*time.Millisecond, 0)
	values := []float64{0, 0.5, 0.999}
	i := 0
	random := func() float64 {
		v := values[i%len(values)]
		i++
		return v
	}
	d0, ok := tracking.NextDelay(&state, random)
	if !ok || d0 != 0 {
		t.Fatalf("d0=%v ok=%v", d0, ok)
	}
	d1, ok := tracking.NextDelay(&state, random)
	if !ok || d1 != 100*time.Millisecond {
		t.Fatalf("d1=%v ok=%v", d1, ok)
	}
	d2, ok := tracking.NextDelay(&state, random)
	if !ok || d2 != 399*time.Millisecond {
		t.Fatalf("d2=%v ok=%v", d2, ok)
	}
}

func TestBackoffMaxAttempts(t *testing.T) {
	state := tracking.NewBackoff(10*time.Millisecond, 0, 2)
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); !ok {
		t.Fatal()
	}
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); !ok {
		t.Fatal()
	}
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); ok {
		t.Fatal("expected exhausted")
	}
}

func TestBackoffReset(t *testing.T) {
	state := tracking.NewBackoff(10*time.Millisecond, 0, 1)
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); !ok {
		t.Fatal()
	}
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); ok {
		t.Fatal()
	}
	tracking.ResetBackoff(&state)
	if _, ok := tracking.NextDelay(&state, func() float64 { return 0 }); !ok {
		t.Fatal()
	}
}
