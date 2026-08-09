package tracking_test

import (
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestPublishRateSpacing(t *testing.T) {
	if tracking.MaxPublishHz != 50 || tracking.MinPublishIntervalMs != 20 {
		t.Fatalf("%d %d", tracking.MaxPublishHz, tracking.MinPublishIntervalMs)
	}
	zero := time.Time{}
	if !tracking.CanAcceptPublish(zero, zero, 1) {
		t.Fatal()
	}
	next := tracking.NextPublishAllowedAt(zero, zero, 1)
	if !next.Equal(zero.Add(20 * time.Millisecond)) {
		t.Fatalf("%v", next)
	}
	if tracking.CanAcceptPublish(next, zero.Add(19*time.Millisecond), 1) {
		t.Fatal("too early")
	}
	if !tracking.CanAcceptPublish(next, zero.Add(20*time.Millisecond), 1) {
		t.Fatal("should allow")
	}
}

func TestPublishRateBatchSlots(t *testing.T) {
	base := time.Unix(0, 0)
	now := base.Add(1000 * time.Millisecond)
	next := tracking.NextPublishAllowedAt(time.Time{}, now, 50)
	want := now.Add(50 * tracking.MinPublishInterval)
	if !next.Equal(want) {
		t.Fatalf("got %v want %v", next, want)
	}
	if tracking.CanAcceptPublish(next, now.Add(999*time.Millisecond), 1) {
		t.Fatal()
	}
	if !tracking.CanAcceptPublish(next, now.Add(1000*time.Millisecond), 1) {
		t.Fatal()
	}
}
