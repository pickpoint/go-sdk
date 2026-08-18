package tracking

import "math"

// InFlightPoint is a filtered point that already has a seq, waiting for Ack.
type InFlightPoint struct {
	Seq   uint32
	Point LatLng
}

// Buffer is Staging + InFlight. A point is in one list, not both.
// Cap is MaxBufferPoints (or the constructor maxSize) across both lists.
type Buffer struct {
	maxSize  int
	staging  []LatLng
	inFlight []InFlightPoint
	onGap    func(dropped int)
}

// NewBuffer creates a send buffer (default cap 10_000).
func NewBuffer(maxSize int, onGap func(dropped int)) *Buffer {
	if maxSize <= 0 {
		maxSize = MaxBufferPoints
	}
	return &Buffer{maxSize: maxSize, onGap: onGap}
}

// NewOfflineQueue is an alias for NewBuffer (older name).
func NewOfflineQueue(maxSize int, onGap func(dropped int)) *Buffer {
	return NewBuffer(maxSize, onGap)
}

func (b *Buffer) StagingSize() int  { return len(b.staging) }
func (b *Buffer) InFlightSize() int { return len(b.inFlight) }
func (b *Buffer) Size() int         { return len(b.staging) + len(b.inFlight) }

func (b *Buffer) PushStaging(p LatLng) {
	b.staging = append(b.staging, p)
	b.enforceCap()
}

func (b *Buffer) PeekStaging() []LatLng {
	out := make([]LatLng, len(b.staging))
	copy(out, b.staging)
	return out
}

func (b *Buffer) PeekInFlight() []InFlightPoint {
	out := make([]InFlightPoint, len(b.inFlight))
	copy(out, b.inFlight)
	return out
}

// AckThrough drops InFlight entries with seq <= ack (inclusive).
func (b *Buffer) AckThrough(ack uint32) {
	n := 0
	for _, p := range b.inFlight {
		if p.Seq > ack {
			b.inFlight[n] = p
			n++
		}
	}
	b.inFlight = b.inFlight[:n]
}

func (b *Buffer) Clear() {
	b.staging = b.staging[:0]
	b.inFlight = b.inFlight[:0]
}

// AssignFromStaging moves up to maxFrames worth of Staging points into InFlight,
// assigning seqs starting at *nextSeq+1. Returns the newly assigned points.
func (b *Buffer) AssignFromStaging(nextSeq *uint64, maxFrames int) []InFlightPoint {
	if maxFrames <= 0 || len(b.staging) == 0 {
		return nil
	}
	n := takeFramePoints(b.staging, maxFrames)
	chunk := append([]LatLng(nil), b.staging[:n]...)
	b.staging = append([]LatLng(nil), b.staging[n:]...)
	out := make([]InFlightPoint, n)
	for i, p := range chunk {
		*nextSeq++
		item := InFlightPoint{Seq: uint32(*nextSeq), Point: p}
		out[i] = item
		b.inFlight = append(b.inFlight, item)
	}
	return out
}

func takeFramePoints(pts []LatLng, maxFrames int) int {
	frames := 0
	i := 0
	for frames < maxFrames && i < len(pts) {
		start := i
		prevLat := DegToMicro(pts[i].Latitude)
		prevLon := DegToMicro(pts[i].Longitude)
		i++
		for i < len(pts) && (i-start) < MaxLocPoints {
			lat := DegToMicro(pts[i].Latitude)
			lon := DegToMicro(pts[i].Longitude)
			if !MicroDeltaFits(prevLat, prevLon, lat, lon) {
				break
			}
			prevLat, prevLon = lat, lon
			i++
		}
		frames++
	}
	return i
}

func (b *Buffer) enforceCap() {
	for b.Size() > b.maxSize {
		if b.collapseMiddle() {
			if b.onGap != nil {
				b.onGap(1)
			}
			continue
		}
		// Keep newest: drop oldest (InFlight first, else Staging[0]).
		if len(b.inFlight) > 0 {
			b.inFlight = b.inFlight[1:]
		} else if len(b.staging) > 0 {
			b.staging = b.staging[1:]
		} else {
			return
		}
		if b.onGap != nil {
			b.onGap(1)
		}
	}
}

func (b *Buffer) collapseMiddle() bool {
	nInf := len(b.inFlight)
	nSt := len(b.staging)
	total := nInf + nSt
	if total < 3 {
		return false
	}
	at := func(i int) LatLng {
		if i < nInf {
			return b.inFlight[i].Point
		}
		return b.staging[i-nInf]
	}
	for i := 1; i < total-1; i++ {
		if perpDistM(at(i-1), at(i+1), at(i)) < 2.0 {
			if i < nInf {
				b.inFlight = append(b.inFlight[:i], b.inFlight[i+1:]...)
			} else {
				j := i - nInf
				b.staging = append(b.staging[:j], b.staging[j+1:]...)
			}
			return true
		}
	}
	return false
}

func perpDistM(a, c, b LatLng) float64 {
	// Perpendicular distance of b from segment a→c, metres (equirectangular).
	const r = 6371000.0
	lat0 := a.Latitude * math.Pi / 180
	toXY := func(p LatLng) (x, y float64) {
		y = (p.Latitude - a.Latitude) * math.Pi / 180 * r
		x = (p.Longitude - a.Longitude) * math.Pi / 180 * math.Cos(lat0) * r
		return
	}
	cx, cy := toXY(c)
	bx, by := toXY(b)
	len2 := cx*cx + cy*cy
	if len2 < 1e-6 {
		return haversineM(a, b)
	}
	return math.Abs(bx*cy-by*cx) / math.Sqrt(len2)
}
