package tracking

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"
)

const (
	pfAlt  uint8 = 1 << 0
	pfAcc  uint8 = 1 << 1
	pfTime uint8 = 1 << 4

	latMin int32 = -90_000_000
	latMax int32 = 90_000_000
	lonMin int32 = -180_000_000
	lonMax int32 = 180_000_000
)

var (
	ErrTruncated     = errors.New("tracking: truncated frame")
	ErrInvalidFrame  = errors.New("tracking: invalid frame")
	ErrEmptyLoc      = errors.New("tracking: empty loc")
	ErrDeltaOverflow = errors.New("tracking: intra-frame delta overflows i16")
)

// StampLatLng sets TimestampMs to now when the caller omitted it (Staging / flush).
func StampLatLng(p *LatLng) *LatLng {
	if p == nil {
		return nil
	}
	if p.TimestampMs == nil {
		now := time.Now().UnixMilli()
		p.TimestampMs = &now
	}
	return p
}

type reader struct {
	b []byte
}

func (r *reader) need(n int) ([]byte, error) {
	if len(r.b) < n {
		return nil, ErrTruncated
	}
	head := r.b[:n]
	r.b = r.b[n:]
	return head, nil
}

func (r *reader) u8() (uint8, error) {
	b, err := r.need(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *reader) u16() (uint16, error) {
	b, err := r.need(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.need(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *reader) i16() (int16, error) {
	v, err := r.u16()
	return int16(v), err
}

func (r *reader) i32() (int32, error) {
	v, err := r.u32()
	return int32(v), err
}

func (r *reader) i64() (int64, error) {
	b, err := r.need(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (r *reader) f64() (float64, error) {
	b, err := r.need(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
}

func (r *reader) uuid() (string, error) {
	b, err := r.need(16)
	if err != nil {
		return "", err
	}
	var raw [16]byte
	copy(raw[:], b)
	return formatUUID(raw), nil
}

func (r *reader) uuidOpt() (string, error) {
	b, err := r.need(16)
	if err != nil {
		return "", err
	}
	var raw [16]byte
	copy(raw[:], b)
	if raw == [16]byte{} {
		return "", nil
	}
	return formatUUID(raw), nil
}

func (r *reader) str() (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}
	if int(n) > MaxStringBytes {
		return "", ErrInvalidFrame
	}
	raw, err := r.need(int(n))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.u16()
	if err != nil {
		return nil, err
	}
	if int(n) > MaxStringBytes {
		return nil, ErrInvalidFrame
	}
	raw, err := r.need(int(n))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func putU8(w *[]byte, v uint8)   { *w = append(*w, v) }
func putU16(w *[]byte, v uint16) { *w = binary.LittleEndian.AppendUint16(*w, v) }
func putU32(w *[]byte, v uint32) { *w = binary.LittleEndian.AppendUint32(*w, v) }
func putI16(w *[]byte, v int16)  { putU16(w, uint16(v)) }
func putI32(w *[]byte, v int32)  { putU32(w, uint32(v)) }
func putI64(w *[]byte, v int64)  { *w = binary.LittleEndian.AppendUint64(*w, uint64(v)) }
func putF64(w *[]byte, v float64) {
	*w = binary.LittleEndian.AppendUint64(*w, math.Float64bits(v))
}

func putUUID(w *[]byte, s string) {
	b := parseUUID(s)
	*w = append(*w, b[:]...)
}

func putStr(w *[]byte, s string) {
	b := []byte(s)
	if len(b) > MaxStringBytes {
		b = b[:MaxStringBytes]
	}
	putU16(w, uint16(len(b)))
	*w = append(*w, b...)
}

func putBytes(w *[]byte, b []byte) {
	if len(b) > MaxStringBytes {
		b = b[:MaxStringBytes]
	}
	putU16(w, uint16(len(b)))
	*w = append(*w, b...)
}

func parseUUID(s string) [16]byte {
	var out [16]byte
	if s == "" {
		return out
	}
	h := strings.ReplaceAll(s, "-", "")
	if len(h) != 32 {
		return out
	}
	raw, err := hex.DecodeString(h)
	if err != nil || len(raw) != 16 {
		return out
	}
	copy(out[:], raw)
	return out
}

func formatUUID(b [16]byte) string {
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// DegToMicro converts degrees to integer microdegrees.
func DegToMicro(d float64) int32 {
	return int32(math.Round(d * 1_000_000.0))
}

// MicroToDeg converts microdegrees to degrees.
func MicroToDeg(m int32) float64 {
	return float64(m) / 1_000_000.0
}

func checkCoord(lat, lon int32) error {
	if lat < latMin || lat > latMax || lon < lonMin || lon > lonMax {
		return ErrInvalidFrame
	}
	return nil
}

// MicroDeltaFits reports whether next-prev fits in an i16 microdegree delta.
func MicroDeltaFits(prevLat, prevLon, lat, lon int32) bool {
	dlat := int64(lat) - int64(prevLat)
	dlon := int64(lon) - int64(prevLon)
	return dlat >= math.MinInt16 && dlat <= math.MaxInt16 &&
		dlon >= math.MinInt16 && dlon <= math.MaxInt16
}

func writePoint(w *[]byte, p LatLng, prev *[2]int32) ([2]int32, error) {
	lat := DegToMicro(p.Latitude)
	lon := DegToMicro(p.Longitude)
	var flags uint8
	if p.Altitude != nil {
		flags |= pfAlt
	}
	if p.Accuracy != nil {
		flags |= pfAcc
	}
	if p.TimestampMs != nil {
		flags |= pfTime
	}
	putU8(w, flags)
	if prev != nil {
		if !MicroDeltaFits(prev[0], prev[1], lat, lon) {
			return [2]int32{}, ErrDeltaOverflow
		}
		putI16(w, int16(lat-prev[0]))
		putI16(w, int16(lon-prev[1]))
	} else {
		putI32(w, lat)
		putI32(w, lon)
	}
	if p.Altitude != nil {
		putI32(w, int32(math.Round(*p.Altitude*1000.0)))
	}
	if p.Accuracy != nil {
		cm := math.Round(*p.Accuracy * 100.0)
		if cm < 0 {
			cm = 0
		}
		if cm > math.MaxUint16 {
			cm = math.MaxUint16
		}
		putU16(w, uint16(cm))
	}
	if p.TimestampMs != nil {
		putI64(w, *p.TimestampMs)
	}
	return [2]int32{lat, lon}, nil
}

func writeAbs(w *[]byte, p LatLng) {
	_, _ = writePoint(w, p, nil)
}

func readPoint(r *reader, prev *[2]int32) (LatLng, [2]int32, error) {
	flags, err := r.u8()
	if err != nil {
		return LatLng{}, [2]int32{}, err
	}
	var lat, lon int32
	if prev != nil {
		dlat, err := r.i16()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		dlon, err := r.i16()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		lat = satAddI32(prev[0], int32(dlat))
		lon = satAddI32(prev[1], int32(dlon))
	} else {
		lat, err = r.i32()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		lon, err = r.i32()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
	}
	if err := checkCoord(lat, lon); err != nil {
		return LatLng{}, [2]int32{}, err
	}
	p := LatLng{Latitude: MicroToDeg(lat), Longitude: MicroToDeg(lon)}
	if flags&pfAlt != 0 {
		mm, err := r.i32()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		alt := float64(mm) / 1000.0
		p.Altitude = &alt
	}
	if flags&pfAcc != 0 {
		cm, err := r.u16()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		acc := float64(cm) / 100.0
		p.Accuracy = &acc
	}
	if flags&pfTime != 0 {
		t, err := r.i64()
		if err != nil {
			return LatLng{}, [2]int32{}, err
		}
		p.TimestampMs = &t
	}
	return p, [2]int32{lat, lon}, nil
}

func satAddI32(a, b int32) int32 {
	s := int64(a) + int64(b)
	if s > math.MaxInt32 {
		return math.MaxInt32
	}
	if s < math.MinInt32 {
		return math.MinInt32
	}
	return int32(s)
}

func writeRouteAbs(w *[]byte, route []LatLng) {
	n := len(route)
	if n > math.MaxUint16 {
		n = math.MaxUint16
	}
	putU16(w, uint16(n))
	for i := 0; i < n; i++ {
		putI32(w, DegToMicro(route[i].Latitude))
		putI32(w, DegToMicro(route[i].Longitude))
	}
}

func readRouteAbs(r *reader) ([]LatLng, error) {
	n, err := r.u16()
	if err != nil {
		return nil, err
	}
	out := make([]LatLng, 0, n)
	for i := 0; i < int(n); i++ {
		lat, err := r.i32()
		if err != nil {
			return nil, err
		}
		lon, err := r.i32()
		if err != nil {
			return nil, err
		}
		if err := checkCoord(lat, lon); err != nil {
			return nil, err
		}
		out = append(out, LatLng{Latitude: MicroToDeg(lat), Longitude: MicroToDeg(lon)})
	}
	return out, nil
}

// EncodeLocFrames splits points into Loc frames (count 1…100, i16 Δ must fit).
// lastSeq is the seq of the last point (same as the Loc seq field).
func EncodeLocFrames(lastSeq uint32, points []LatLng) [][]byte {
	if len(points) == 0 {
		return nil
	}
	n := uint32(len(points))
	firstSeq := lastSeq + 1 - n
	var out [][]byte
	i := 0
	for i < len(points) {
		start := i
		prevLat := DegToMicro(points[i].Latitude)
		prevLon := DegToMicro(points[i].Longitude)
		i++
		for i < len(points) && (i-start) < MaxLocPoints {
			lat := DegToMicro(points[i].Latitude)
			lon := DegToMicro(points[i].Longitude)
			if !MicroDeltaFits(prevLat, prevLon, lat, lon) {
				break
			}
			prevLat, prevLon = lat, lon
			i++
		}
		chunk := points[start:i]
		seq := firstSeq + uint32(i) - 1
		out = append(out, mustEncodeLocFrame(seq, chunk))
	}
	return out
}

func mustEncodeLocFrame(seq uint32, points []LatLng) []byte {
	b, err := encodeLocFrame(seq, points)
	if err != nil {
		// chunk already checked for Δ and count
		return nil
	}
	return b
}

func encodeLocFrame(seq uint32, points []LatLng) ([]byte, error) {
	if len(points) == 0 {
		return nil, ErrEmptyLoc
	}
	if len(points) > MaxLocPoints {
		return nil, ErrInvalidFrame
	}
	w := []byte{TypeLoc}
	putU32(&w, seq)
	putU8(&w, uint8(len(points)))
	var prev *[2]int32
	var cur [2]int32
	for _, p := range points {
		var err error
		cur, err = writePoint(&w, p, prev)
		if err != nil {
			return nil, err
		}
		cp := cur
		prev = &cp
	}
	return w, nil
}

func locPointsFromInFlight(pts []InFlightPoint) []LatLng {
	out := make([]LatLng, len(pts))
	for i, p := range pts {
		out[i] = p.Point
	}
	return out
}

// EncodeInFlightFrames packs already-numbered points into Loc frames.
// Contiguous seqs stay in one frame until count=100 or i16 Δ overflow.
func EncodeInFlightFrames(pts []InFlightPoint) [][]byte {
	if len(pts) == 0 {
		return nil
	}
	var out [][]byte
	i := 0
	for i < len(pts) {
		start := i
		prevLat := DegToMicro(pts[i].Point.Latitude)
		prevLon := DegToMicro(pts[i].Point.Longitude)
		i++
		for i < len(pts) && (i-start) < MaxLocPoints {
			if pts[i].Seq != pts[i-1].Seq+1 {
				break
			}
			lat := DegToMicro(pts[i].Point.Latitude)
			lon := DegToMicro(pts[i].Point.Longitude)
			if !MicroDeltaFits(prevLat, prevLon, lat, lon) {
				break
			}
			prevLat, prevLon = lat, lon
			i++
		}
		chunk := pts[start:i]
		out = append(out, mustEncodeLocFrame(chunk[len(chunk)-1].Seq, locPointsFromInFlight(chunk)))
	}
	return out
}

// EncodeClientMsg marshals a client frame to little-endian bytes.
func EncodeClientMsg(msg ClientMsg) ([]byte, error) {
	var w []byte
	switch {
	case msg.Resume != nil:
		putU8(&w, TypeResume)
		putUUID(&w, msg.Resume.TrackUID)
		putU32(&w, msg.Resume.LastSeq)
	case msg.TrackStart != nil:
		putU8(&w, TypeTrackStart)
		var flags uint8
		if msg.TrackStart.Location != nil {
			flags |= 1
		}
		putU8(&w, flags)
		if msg.TrackStart.Location != nil {
			writeAbs(&w, *msg.TrackStart.Location)
		}
		writeRouteAbs(&w, msg.TrackStart.Route)
		putBytes(&w, msg.TrackStart.Metadata)
	case msg.TrackStop != nil:
		putU8(&w, TypeTrackStop)
	case msg.Loc != nil:
		return encodeLocFrame(msg.Loc.Seq, msg.Loc.Points)
	case msg.Subscribe != nil:
		putU8(&w, TypeSubscribe)
		putUUID(&w, msg.Subscribe.DeviceUID)
		var flags uint8
		if msg.Subscribe.IncludeEvents {
			flags = 1
		}
		putU8(&w, flags)
		putU16(&w, msg.Subscribe.MinIntervalMs)
	case msg.Unsubscribe != nil:
		putU8(&w, TypeUnsubscribe)
		putU8(&w, msg.Unsubscribe.Sub)
	case msg.Event != nil:
		putU8(&w, TypeEvent)
		putBytes(&w, msg.Event.Payload)
		putI64(&w, msg.Event.TimestampMs)
	case msg.CommandAck != nil:
		putU8(&w, TypeCommandAck)
		putUUID(&w, msg.CommandAck.CommandID)
		putU8(&w, uint8(msg.CommandAck.Status))
		putStr(&w, msg.CommandAck.Message)
	default:
		return nil, ErrInvalidFrame
	}
	return w, nil
}

// DecodeClientMsg unmarshals a client frame. Extra trailing bytes are ignored.
func DecodeClientMsg(data []byte) (ClientMsg, error) {
	if len(data) == 0 {
		return ClientMsg{}, ErrTruncated
	}
	r := reader{b: data}
	typ, err := r.u8()
	if err != nil {
		return ClientMsg{}, err
	}
	switch typ {
	case TypeResume:
		uid, err := r.uuid()
		if err != nil {
			return ClientMsg{}, err
		}
		seq, err := r.u32()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{Resume: &Resume{TrackUID: uid, LastSeq: seq}}, nil
	case TypeTrackStart:
		flags, err := r.u8()
		if err != nil {
			return ClientMsg{}, err
		}
		var loc *LatLng
		if flags&1 != 0 {
			p, _, err := readPoint(&r, nil)
			if err != nil {
				return ClientMsg{}, err
			}
			loc = &p
		}
		route, err := readRouteAbs(&r)
		if err != nil {
			return ClientMsg{}, err
		}
		meta, err := r.bytes()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{TrackStart: &TrackStart{Location: loc, Route: route, Metadata: meta}}, nil
	case TypeTrackStop:
		return ClientMsg{TrackStop: &TrackStop{}}, nil
	case TypeLoc:
		seq, err := r.u32()
		if err != nil {
			return ClientMsg{}, err
		}
		count, err := r.u8()
		if err != nil {
			return ClientMsg{}, err
		}
		if count == 0 || count > MaxLocPoints {
			return ClientMsg{}, ErrInvalidFrame
		}
		points := make([]LatLng, 0, count)
		var prev *[2]int32
		var cur [2]int32
		for i := 0; i < int(count); i++ {
			p, xy, err := readPoint(&r, prev)
			if err != nil {
				return ClientMsg{}, err
			}
			points = append(points, p)
			cur = xy
			prev = &cur
		}
		return ClientMsg{Loc: &Loc{Seq: seq, Points: points}}, nil
	case TypeSubscribe:
		uid, err := r.uuid()
		if err != nil {
			return ClientMsg{}, err
		}
		flags, err := r.u8()
		if err != nil {
			return ClientMsg{}, err
		}
		iv, err := r.u16()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{Subscribe: &Subscribe{
			DeviceUID: uid, IncludeEvents: flags&1 != 0, MinIntervalMs: iv,
		}}, nil
	case TypeUnsubscribe:
		sub, err := r.u8()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{Unsubscribe: &Unsubscribe{Sub: sub}}, nil
	case TypeEvent:
		payload, err := r.bytes()
		if err != nil {
			return ClientMsg{}, err
		}
		ts, err := r.i64()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{Event: &Event{Payload: payload, TimestampMs: ts}}, nil
	case TypeCommandAck:
		id, err := r.uuid()
		if err != nil {
			return ClientMsg{}, err
		}
		st, err := r.u8()
		if err != nil {
			return ClientMsg{}, err
		}
		msg, err := r.str()
		if err != nil {
			return ClientMsg{}, err
		}
		return ClientMsg{CommandAck: &CommandAck{
			CommandID: id, Status: commandAckFromU8(st), Message: msg,
		}}, nil
	case 0x00, 0x7F, 0xFF:
		return ClientMsg{}, ErrInvalidFrame
	default:
		if typ >= 0x01 && typ <= 0x7E {
			t := typ
			return ClientMsg{Unknown: &t}, nil
		}
		return ClientMsg{}, ErrInvalidFrame
	}
}

// EncodeServerMsg marshals a server frame (used by tests / mock server).
func EncodeServerMsg(msg ServerMsg) ([]byte, error) {
	var w []byte
	switch {
	case msg.Hello != nil:
		putU8(&w, TypeHello)
		putU8(&w, msg.Hello.Version)
		putU16(&w, msg.Hello.Shard)
		putUUID(&w, msg.Hello.NodeID)
	case msg.Relocate != nil:
		putU8(&w, TypeRelocate)
		putU32(&w, msg.Relocate.RetryAfterMs)
		putStr(&w, msg.Relocate.Endpoint)
	case msg.ResumeOk != nil:
		putU8(&w, TypeResumeOk)
		putUUID(&w, msg.ResumeOk.TrackUID)
		putU32(&w, msg.ResumeOk.LastAcked)
	case msg.TrackStarted != nil:
		putU8(&w, TypeTrackStarted)
		putUUID(&w, msg.TrackStarted.TrackUID)
		putBytes(&w, msg.TrackStarted.Metadata)
	case msg.TrackStopped != nil:
		putU8(&w, TypeTrackStopped)
		putUUID(&w, msg.TrackStopped.TrackUID)
	case msg.Ack != nil:
		putU8(&w, TypeAck)
		putU32(&w, msg.Ack.Seq)
	case msg.Loc != nil:
		putU8(&w, TypeServerLoc)
		putU8(&w, msg.Loc.Sub)
		putU32(&w, msg.Loc.Seq)
		writeAbs(&w, msg.Loc.Point)
	case msg.Subscribed != nil:
		s := msg.Subscribed
		putU8(&w, TypeSubscribed)
		putU8(&w, s.Sub)
		putUUID(&w, s.DeviceUID)
		putUUID(&w, s.TrackUID)
		var on uint8
		if s.Online {
			on = 1
		}
		putU8(&w, on)
		var flags uint8
		if s.LastLocation != nil {
			flags |= 1
		}
		if s.LastSeenMs != nil {
			flags |= 2
		}
		if len(s.Route) > 0 {
			flags |= 4
		}
		putU8(&w, flags)
		if s.LastLocation != nil {
			writeAbs(&w, *s.LastLocation)
		}
		if s.LastSeenMs != nil {
			putI64(&w, *s.LastSeenMs)
		}
		if flags&4 != 0 {
			writeRouteAbs(&w, s.Route)
		}
		putF64(&w, s.EstDistance)
		putF64(&w, s.EstDuration)
		putStr(&w, s.StartName)
		putStr(&w, s.EndName)
		putBytes(&w, s.Metadata)
	case msg.Error != nil:
		putU8(&w, TypeError)
		putU8(&w, uint8(msg.Error.Code))
		putU32(&w, msg.Error.RetryAfterMs)
		putUUID(&w, msg.Error.TrackUID)
		putStr(&w, msg.Error.Message)
	case msg.EventAdded != nil:
		putU8(&w, TypeEventAdded)
		putU8(&w, msg.EventAdded.Sub)
		putBytes(&w, msg.EventAdded.Payload)
		putI64(&w, msg.EventAdded.TimestampMs)
	case msg.Command != nil:
		putU8(&w, TypeCommand)
		putUUID(&w, msg.Command.CommandID)
		putBytes(&w, msg.Command.Payload)
		putI64(&w, msg.Command.TimestampMs)
	case msg.Presence != nil:
		putU8(&w, TypePresence)
		putU8(&w, msg.Presence.Sub)
		var on uint8
		if msg.Presence.Online {
			on = 1
		}
		putU8(&w, on)
		putI64(&w, msg.Presence.LastSeenMs)
	default:
		return nil, ErrInvalidFrame
	}
	return w, nil
}

// DecodeServerMsg unmarshals a server frame. Unknown types in 0x80–0xFE return (zero, nil).
func DecodeServerMsg(data []byte) (ServerMsg, error) {
	if len(data) == 0 {
		return ServerMsg{}, ErrTruncated
	}
	r := reader{b: data}
	typ, err := r.u8()
	if err != nil {
		return ServerMsg{}, err
	}
	switch typ {
	case TypeHello:
		ver, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		shard, err := r.u16()
		if err != nil {
			return ServerMsg{}, err
		}
		node, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Hello: &Hello{Version: ver, Shard: shard, NodeID: node}}, nil
	case TypeRelocate:
		ms, err := r.u32()
		if err != nil {
			return ServerMsg{}, err
		}
		ep, err := r.str()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Relocate: &Relocate{RetryAfterMs: ms, Endpoint: ep}}, nil
	case TypeResumeOk:
		uid, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		acked, err := r.u32()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{ResumeOk: &ResumeOk{TrackUID: uid, LastAcked: acked}}, nil
	case TypeTrackStarted:
		uid, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		meta, err := r.bytes()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{TrackStarted: &TrackStarted{TrackUID: uid, Metadata: meta}}, nil
	case TypeTrackStopped:
		uid, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{TrackStopped: &TrackStopped{TrackUID: uid}}, nil
	case TypeAck:
		seq, err := r.u32()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Ack: &Ack{Seq: seq}}, nil
	case TypeServerLoc:
		sub, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		seq, err := r.u32()
		if err != nil {
			return ServerMsg{}, err
		}
		p, _, err := readPoint(&r, nil)
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Loc: &ServerLoc{Sub: sub, Seq: seq, Point: p}}, nil
	case TypeSubscribed:
		sub, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		dev, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		track, err := r.uuidOpt()
		if err != nil {
			return ServerMsg{}, err
		}
		on, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		flags, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		var lastLoc *LatLng
		if flags&1 != 0 {
			p, _, err := readPoint(&r, nil)
			if err != nil {
				return ServerMsg{}, err
			}
			lastLoc = &p
		}
		var lastSeen *int64
		if flags&2 != 0 {
			t, err := r.i64()
			if err != nil {
				return ServerMsg{}, err
			}
			lastSeen = &t
		}
		var route []LatLng
		if flags&4 != 0 {
			route, err = readRouteAbs(&r)
			if err != nil {
				return ServerMsg{}, err
			}
		}
		dist, err := r.f64()
		if err != nil {
			return ServerMsg{}, err
		}
		dur, err := r.f64()
		if err != nil {
			return ServerMsg{}, err
		}
		start, err := r.str()
		if err != nil {
			return ServerMsg{}, err
		}
		end, err := r.str()
		if err != nil {
			return ServerMsg{}, err
		}
		meta, err := r.bytes()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Subscribed: &Subscribed{
			Sub: sub, DeviceUID: dev, TrackUID: track, Online: on != 0,
			LastLocation: lastLoc, LastSeenMs: lastSeen, Route: route,
			EstDistance: dist, EstDuration: dur, StartName: start, EndName: end,
			Metadata: meta,
		}}, nil
	case TypeError:
		codeU8, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		code, ok := errorCodeFromU8(codeU8)
		if !ok {
			return ServerMsg{}, ErrInvalidFrame
		}
		retry, err := r.u32()
		if err != nil {
			return ServerMsg{}, err
		}
		uid, err := r.uuidOpt()
		if err != nil {
			return ServerMsg{}, err
		}
		msg, err := r.str()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Error: &WireError{Code: code, RetryAfterMs: retry, TrackUID: uid, Message: msg}}, nil
	case TypeEventAdded:
		sub, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		payload, err := r.bytes()
		if err != nil {
			return ServerMsg{}, err
		}
		t, err := r.i64()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{EventAdded: &EventAdded{Sub: sub, Payload: payload, TimestampMs: t}}, nil
	case TypeCommand:
		id, err := r.uuid()
		if err != nil {
			return ServerMsg{}, err
		}
		payload, err := r.bytes()
		if err != nil {
			return ServerMsg{}, err
		}
		t, err := r.i64()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Command: &Command{CommandID: id, Payload: payload, TimestampMs: t}}, nil
	case TypePresence:
		sub, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		on, err := r.u8()
		if err != nil {
			return ServerMsg{}, err
		}
		t, err := r.i64()
		if err != nil {
			return ServerMsg{}, err
		}
		return ServerMsg{Presence: &Presence{Sub: sub, Online: on != 0, LastSeenMs: t}}, nil
	case 0x00, 0x7F, 0xFF, 0x8C:
		return ServerMsg{}, ErrInvalidFrame
	default:
		if typ >= 0x80 && typ <= 0xFE {
			return ServerMsg{}, nil
		}
		return ServerMsg{}, ErrInvalidFrame
	}
}

// ClientResume builds a Resume client message (goldens / wire tests).
func ClientResume(trackUID string, lastSeq uint32) ClientMsg {
	return ClientMsg{Resume: &Resume{TrackUID: trackUID, LastSeq: lastSeq}}
}
