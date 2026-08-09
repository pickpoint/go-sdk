package tracking_test

import (
	"testing"

	"github.com/pickpoint/go-sdk/tracking"
	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

func TestOfflineQueueAckThrough(t *testing.T) {
	q := tracking.NewOfflineQueue(10, nil)
	q.Enqueue(1, &pb.LatLng{Latitude: 1})
	q.Enqueue(2, &pb.LatLng{Latitude: 2})
	q.Enqueue(3, &pb.LatLng{Latitude: 3})
	q.AckThrough(2)
	got := q.PeekAll()
	if len(got) != 1 || got[0].Seq != 3 {
		t.Fatalf("%v", got)
	}
}

func TestOfflineQueueDropOldest(t *testing.T) {
	var dropped int
	q := tracking.NewOfflineQueue(2, func(n int) { dropped = n })
	q.Enqueue(1, &pb.LatLng{Latitude: 1})
	q.Enqueue(2, &pb.LatLng{Latitude: 2})
	q.Enqueue(3, &pb.LatLng{Latitude: 3})
	if dropped != 1 {
		t.Fatalf("dropped=%d", dropped)
	}
	got := q.PeekAll()
	if len(got) != 2 || got[0].Seq != 2 || got[1].Seq != 3 {
		t.Fatalf("%v", got)
	}
}
