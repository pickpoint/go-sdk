package tracking

import pb "github.com/pickpoint/go-sdk/tracking/v2"

// QueuedPoint is an offline location pending flush after resume.
type QueuedPoint struct {
	Seq   uint64
	Point *pb.LatLng
}

// OfflineQueue is a bounded offline queue keyed by clientSeq.
// Drop-oldest on overflow; caller is notified via onGap.
type OfflineQueue struct {
	maxSize int
	items   []QueuedPoint
	onGap   func(dropped int)
}

// NewOfflineQueue creates a queue with maxSize capacity (default 10_000).
func NewOfflineQueue(maxSize int, onGap func(dropped int)) *OfflineQueue {
	if maxSize <= 0 {
		maxSize = 10_000
	}
	return &OfflineQueue{maxSize: maxSize, onGap: onGap}
}

func (q *OfflineQueue) Size() int { return len(q.items) }

func (q *OfflineQueue) Enqueue(seq uint64, point *pb.LatLng) {
	q.items = append(q.items, QueuedPoint{Seq: seq, Point: point})
	if len(q.items) > q.maxSize {
		dropped := len(q.items) - q.maxSize
		q.items = q.items[dropped:]
		if q.onGap != nil {
			q.onGap(dropped)
		}
	}
}

// AckThrough drops points with seq <= ack (inclusive).
func (q *OfflineQueue) AckThrough(ack uint64) {
	n := 0
	for _, p := range q.items {
		if p.Seq > ack {
			q.items[n] = p
			n++
		}
	}
	q.items = q.items[:n]
}

func (q *OfflineQueue) PeekAll() []QueuedPoint {
	out := make([]QueuedPoint, len(q.items))
	copy(out, q.items)
	return out
}

func (q *OfflineQueue) Clear() {
	q.items = q.items[:0]
}
