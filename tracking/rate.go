package tracking

import "time"

// MinPublishIntervalMs is the millisecond form of MinPublishInterval (JS parity).
const MinPublishIntervalMs = 1000 / MaxPublishHz // 20

// CanAcceptPublish reports whether pointCount points can be accepted at now.
func CanAcceptPublish(nextAllowedAt, now time.Time, pointCount int) bool {
	if pointCount <= 0 {
		return true
	}
	return !now.Before(nextAllowedAt)
}

// NextPublishAllowedAt advances the gate after accepting pointCount points at now.
func NextPublishAllowedAt(nextAllowedAt, now time.Time, pointCount int) time.Time {
	start := now
	if nextAllowedAt.After(start) {
		start = nextAllowedAt
	}
	if pointCount < 0 {
		pointCount = 0
	}
	return start.Add(MinPublishInterval * time.Duration(pointCount))
}
