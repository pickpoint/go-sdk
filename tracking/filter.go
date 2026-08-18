package tracking

import (
	"math"
	"time"
)

const (
	filterHeartbeat   = time.Second
	filterMinMoveM    = 2.0
	filterHeadingJump = 25.0
	filterStopSpeed   = 0.5 // m/s
	earthRadiusM      = 6_371_000.0
)

// NoiseFilter drops collinear / high-rate GPS before Staging / seq.
type NoiseFilter struct {
	lastEmitted *LatLng
	candidate   *LatLng
	lastEmitAt  time.Time
}

// Push considers one GPS sample. ok means the point should enter Staging or a Loc.
func (f *NoiseFilter) Push(p LatLng, now time.Time) (LatLng, bool) {
	t := now
	if p.TimestampMs != nil {
		t = time.UnixMilli(*p.TimestampMs)
	}
	if f.lastEmitted == nil {
		f.emit(p, t)
		return p, true
	}
	if !f.lastEmitAt.IsZero() && t.Sub(f.lastEmitAt) >= filterHeartbeat {
		f.emit(p, t)
		return p, true
	}
	acc := 0.0
	if p.Accuracy != nil {
		acc = *p.Accuracy
	}
	if haversineM(*f.lastEmitted, p) >= math.Max(filterMinMoveM, 2*acc) {
		f.emit(p, t)
		return p, true
	}
	if f.headingJump(*f.lastEmitted, p) {
		f.emit(p, t)
		return p, true
	}
	if f.motionEdge(*f.lastEmitted, p) {
		f.emit(p, t)
		return p, true
	}
	if f.candidate != nil {
		speed := 0.0
		if p.Speed != nil {
			speed = *p.Speed
		}
		eps := math.Max(filterMinMoveM, acc)
		eps = math.Max(eps, 0.5*speed)
		if perpDistM(*f.lastEmitted, p, *f.candidate) >= eps {
			emitted := *f.candidate
			f.emit(emitted, t)
			return emitted, true
		}
	}
	cp := p
	f.candidate = &cp
	return LatLng{}, false
}

func (f *NoiseFilter) Reset() {
	f.lastEmitted = nil
	f.candidate = nil
	f.lastEmitAt = time.Time{}
}

func (f *NoiseFilter) emit(p LatLng, t time.Time) {
	cp := p
	f.lastEmitted = &cp
	f.candidate = nil
	f.lastEmitAt = t
}

func (f *NoiseFilter) headingJump(prev, cur LatLng) bool {
	if prev.Heading == nil || cur.Heading == nil {
		return false
	}
	return headingDelta(*prev.Heading, *cur.Heading) >= filterHeadingJump
}

func (f *NoiseFilter) motionEdge(prev, cur LatLng) bool {
	if prev.Speed == nil || cur.Speed == nil {
		return false
	}
	return isStopped(*prev.Speed) != isStopped(*cur.Speed)
}

func isStopped(speed float64) bool {
	return speed < filterStopSpeed
}

func headingDelta(a, b float64) float64 {
	d := math.Abs(b - a)
	if d > 180 {
		d = 360 - d
	}
	return d
}

func haversineM(a, b LatLng) float64 {
	φ1 := a.Latitude * math.Pi / 180
	φ2 := b.Latitude * math.Pi / 180
	Δφ := (b.Latitude - a.Latitude) * math.Pi / 180
	Δλ := (b.Longitude - a.Longitude) * math.Pi / 180
	s := math.Sin(Δφ/2)*math.Sin(Δφ/2) + math.Cos(φ1)*math.Cos(φ2)*math.Sin(Δλ/2)*math.Sin(Δλ/2)
	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(s)))
}
