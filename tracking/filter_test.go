package tracking_test

import (
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestFilterFirstPointAlwaysEmits(t *testing.T) {
	var f tracking.NoiseFilter
	now := time.Unix(1_700_000_000, 0)
	_, ok := f.Push(tracking.LatLng{Latitude: 55, Longitude: 37}, now)
	if !ok {
		t.Fatal("first point")
	}
}

func TestFilterDropsNearbyBurst(t *testing.T) {
	var f tracking.NoiseFilter
	now := time.Unix(1_700_000_000, 0)
	_, ok := f.Push(tracking.LatLng{Latitude: 55, Longitude: 37}, now)
	if !ok {
		t.Fatal("first")
	}
	// ~0.5 m east, 10 ms later — should stay a candidate.
	_, ok = f.Push(tracking.LatLng{Latitude: 55, Longitude: 37.000005}, now.Add(10*time.Millisecond))
	if ok {
		t.Fatal("nearby burst should be filtered")
	}
}

func TestFilterHeartbeatEmitsCurrent(t *testing.T) {
	var f tracking.NoiseFilter
	now := time.Unix(1_700_000_000, 0)
	f.Push(tracking.LatLng{Latitude: 55, Longitude: 37}, now)
	p, ok := f.Push(tracking.LatLng{Latitude: 55.000001, Longitude: 37}, now.Add(time.Second))
	if !ok {
		t.Fatal("heartbeat")
	}
	if p.Latitude != 55.000001 {
		t.Fatalf("want current position, got %v", p)
	}
}

func TestFilterHeadingJumpEmits(t *testing.T) {
	var f tracking.NoiseFilter
	now := time.Unix(1_700_000_000, 0)
	h0, h1 := 0.0, 40.0
	f.Push(tracking.LatLng{Latitude: 55, Longitude: 37, Heading: &h0}, now)
	_, ok := f.Push(tracking.LatLng{Latitude: 55.000001, Longitude: 37, Heading: &h1}, now.Add(20*time.Millisecond))
	if !ok {
		t.Fatal("heading jump ≥ 25°")
	}
}
