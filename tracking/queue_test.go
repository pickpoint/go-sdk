package tracking_test

import (
	"testing"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestBufferAckThroughInFlightOnly(t *testing.T) {
	q := tracking.NewBuffer(10, nil)
	q.PushStaging(tracking.LatLng{Latitude: 1})
	next := uint64(0)
	assigned := q.AssignFromStaging(&next, 8)
	if len(assigned) != 1 || assigned[0].Seq != 1 {
		t.Fatalf("%v", assigned)
	}
	q.PushStaging(tracking.LatLng{Latitude: 2})
	q.AckThrough(1)
	if q.InFlightSize() != 0 {
		t.Fatalf("inflight=%d", q.InFlightSize())
	}
	if q.StagingSize() != 1 {
		t.Fatalf("staging=%d", q.StagingSize())
	}
}

func TestBufferOverflowCollapsesMiddle(t *testing.T) {
	var dropped int
	q := tracking.NewBuffer(3, func(n int) { dropped += n })
	// Three collinear points on the equator, then a 4th forces collapse/drop.
	q.PushStaging(tracking.LatLng{Latitude: 0, Longitude: 0})
	q.PushStaging(tracking.LatLng{Latitude: 0, Longitude: 0.00001})
	q.PushStaging(tracking.LatLng{Latitude: 0, Longitude: 0.00002})
	q.PushStaging(tracking.LatLng{Latitude: 0, Longitude: 0.00003})
	if q.Size() != 3 {
		t.Fatalf("size=%d", q.Size())
	}
	if dropped == 0 {
		t.Fatal("expected overflow drop")
	}
	got := q.PeekStaging()
	last := got[len(got)-1]
	if last.Longitude < 0.00002 {
		t.Fatalf("newest not kept: %v", got)
	}
}

func TestAssignSeqAfterStaging(t *testing.T) {
	q := tracking.NewBuffer(10, nil)
	q.PushStaging(tracking.LatLng{Latitude: 1})
	q.PushStaging(tracking.LatLng{Latitude: 1.001})
	if q.InFlightSize() != 0 {
		t.Fatal("seq assigned too early")
	}
	next := uint64(40)
	assigned := q.AssignFromStaging(&next, 8)
	if len(assigned) != 2 || assigned[0].Seq != 41 || assigned[1].Seq != 42 || next != 42 {
		t.Fatalf("%v next=%d", assigned, next)
	}
	if q.StagingSize() != 0 {
		t.Fatal("staging should be empty")
	}
}
