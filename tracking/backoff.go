package tracking

import (
	"math"
	"math/rand"
	"time"
)

// BackoffState tracks full-jitter exponential reconnect delays.
type BackoffState struct {
	Attempt     int
	MinDelay    time.Duration
	MaxDelay    time.Duration
	MaxAttempts int // 0 or negative = unlimited
}

// NewBackoff builds backoff from reconnect options (defaults: 500ms … 30s).
func NewBackoff(minDelay, maxDelay time.Duration, maxAttempts int) BackoffState {
	if minDelay <= 0 {
		minDelay = 500 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	return BackoffState{
		MinDelay:    minDelay,
		MaxDelay:    maxDelay,
		MaxAttempts: maxAttempts,
	}
}

// NextDelay returns the next sleep duration, or false when attempts are exhausted.
// random in [0,1); pass nil for rand.Float64.
// Uses millisecond caps (JS full-jitter parity) then converts to time.Duration.
func NextDelay(state *BackoffState, random func() float64) (time.Duration, bool) {
	if state.MaxAttempts > 0 && state.Attempt >= state.MaxAttempts {
		return 0, false
	}
	if random == nil {
		random = rand.Float64
	}
	minMs := float64(state.MinDelay / time.Millisecond)
	maxMs := float64(state.MaxDelay / time.Millisecond)
	exp := minMs * math.Pow(2, float64(state.Attempt))
	if exp > maxMs {
		exp = maxMs
	}
	state.Attempt++
	return time.Duration(math.Floor(random()*exp)) * time.Millisecond, true
}

// ResetBackoff clears the attempt counter.
func ResetBackoff(state *BackoffState) {
	state.Attempt = 0
}
