package tracking_test

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestStampLatLngDefaultTimestamp(t *testing.T) {
	before := time.Now().UnixMilli()
	p := tracking.StampLatLng(&tracking.LatLng{Latitude: 1, Longitude: 2})
	after := time.Now().UnixMilli()
	if p.TimestampMs == nil {
		t.Fatal("nil ts")
	}
	ts := *p.TimestampMs
	if ts < before || ts > after {
		t.Fatalf("%d not in [%d,%d]", ts, before, after)
	}
}

func TestStampLatLngPreservesTimestamp(t *testing.T) {
	var ts int64 = 42
	p := tracking.StampLatLng(&tracking.LatLng{Latitude: 1, Longitude: 2, TimestampMs: &ts})
	if p.TimestampMs == nil || *p.TimestampMs != 42 {
		t.Fatalf("%v", p.TimestampMs)
	}
}

func TestGoldenAckSeq1(t *testing.T) {
	b, err := tracking.EncodeServerMsg(tracking.ServerMsg{Ack: &tracking.Ack{Seq: 1}})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("8501000000")
	if !bytes.Equal(b, want) {
		t.Fatalf("got %x want %x", b, want)
	}
	msg, err := tracking.DecodeServerMsg(b)
	if err != nil || msg.Ack == nil || msg.Ack.Seq != 1 {
		t.Fatalf("%v %v", msg, err)
	}
}

func TestGoldenLoc55N37E(t *testing.T) {
	b, err := tracking.EncodeClientMsg(tracking.ClientMsg{Loc: &tracking.Loc{
		Seq:    1,
		Points: []tracking.LatLng{{Latitude: 55, Longitude: 37}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("04010000000100c03b470340933402")
	if !bytes.Equal(b, want) {
		t.Fatalf("got %x want %x", b, want)
	}
}

func TestGoldenResume(t *testing.T) {
	msg := tracking.ClientResume("00112233-4455-6677-8899-aabbccddeeff", 45)
	b, err := tracking.EncodeClientMsg(msg)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("0100112233445566778899aabbccddeeff2d000000")
	if !bytes.Equal(b, want) {
		t.Fatalf("got %x want %x", b, want)
	}
}

func TestGoldenTrackStop(t *testing.T) {
	b, err := tracking.EncodeClientMsg(tracking.ClientMsg{TrackStop: &tracking.TrackStop{}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{0x03}) {
		t.Fatalf("%x", b)
	}
}

func TestCodecRoundTripResume(t *testing.T) {
	msg := tracking.ClientResume("00112233-4455-6677-8899-aabbccddeeff", 9)
	b, err := tracking.EncodeClientMsg(msg)
	if err != nil {
		t.Fatal(err)
	}
	round, err := tracking.DecodeClientMsg(b)
	if err != nil {
		t.Fatal(err)
	}
	if round.Resume == nil || round.Resume.TrackUID != "00112233-4455-6677-8899-aabbccddeeff" || round.Resume.LastSeq != 9 {
		t.Fatalf("%+v", round.Resume)
	}
}

func TestCodecRoundTripHello(t *testing.T) {
	msg := tracking.ServerMsg{Hello: &tracking.Hello{Version: 2, NodeID: mockNodeID, Shard: 7}}
	b, err := tracking.EncodeServerMsg(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracking.DecodeServerMsg(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Hello == nil || got.Hello.NodeID != mockNodeID || got.Hello.Shard != 7 || got.Hello.Version != 2 {
		t.Fatalf("%+v", got.Hello)
	}
}

func TestDeviceAckVsListenerLocTypes(t *testing.T) {
	ack, _ := tracking.EncodeServerMsg(tracking.ServerMsg{Ack: &tracking.Ack{Seq: 1}})
	if ack[0] != tracking.TypeAck {
		t.Fatalf("ack type %02x", ack[0])
	}
	loc, _ := tracking.EncodeServerMsg(tracking.ServerMsg{Loc: &tracking.ServerLoc{
		Sub: 1, Seq: 1, Point: tracking.LatLng{Latitude: 55, Longitude: 37},
	}})
	if loc[0] != tracking.TypeServerLoc {
		t.Fatalf("loc type %02x", loc[0])
	}
	if ack[0] == loc[0] {
		t.Fatal("device Ack and listener Loc must differ")
	}
}

func TestEncodeLocSplitsOnI16Overflow(t *testing.T) {
	a := tracking.LatLng{Latitude: 0, Longitude: 0}
	b := tracking.LatLng{Latitude: 4, Longitude: 0} // 4e6 μ° > i16 max
	if tracking.MicroDeltaFits(
		tracking.DegToMicro(0), tracking.DegToMicro(0),
		tracking.DegToMicro(4), tracking.DegToMicro(0),
	) {
		t.Fatal("expected overflow")
	}
	frames := tracking.EncodeLocFrames(2, []tracking.LatLng{a, b})
	if len(frames) != 2 {
		t.Fatalf("frames=%d", len(frames))
	}
	m0, err := tracking.DecodeClientMsg(frames[0])
	if err != nil || m0.Loc == nil || m0.Loc.Seq != 1 {
		t.Fatalf("frame0 %+v %v", m0.Loc, err)
	}
	m1, err := tracking.DecodeClientMsg(frames[1])
	if err != nil || m1.Loc == nil || m1.Loc.Seq != 2 {
		t.Fatalf("frame1 %+v %v", m1.Loc, err)
	}
	if m1.Loc.Points[0].Latitude != 4 {
		t.Fatalf("%v", m1.Loc.Points[0])
	}
}

func TestUnknownServerTypeIgnored(t *testing.T) {
	msg, err := tracking.DecodeServerMsg([]byte{0x8D})
	if err != nil {
		t.Fatal(err)
	}
	if msg != (tracking.ServerMsg{}) {
		t.Fatalf("%+v", msg)
	}
}

func TestUnknownClientTypeNotFatal(t *testing.T) {
	msg, err := tracking.DecodeClientMsg([]byte{0x09})
	if err != nil || msg.Unknown == nil || *msg.Unknown != 0x09 {
		t.Fatalf("%+v %v", msg, err)
	}
}

func TestTrailingBytesIgnored(t *testing.T) {
	b, _ := tracking.EncodeClientMsg(tracking.ClientMsg{TrackStop: &tracking.TrackStop{}})
	b = append(b, 0xDE, 0xAD)
	msg, err := tracking.DecodeClientMsg(b)
	if err != nil || msg.TrackStop == nil {
		t.Fatalf("%+v %v", msg, err)
	}
}

func TestUnsubscribeIsSubHandle(t *testing.T) {
	b, err := tracking.EncodeClientMsg(tracking.ClientMsg{Unsubscribe: &tracking.Unsubscribe{Sub: 7}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte{0x06, 0x07}) {
		t.Fatalf("%x", b)
	}
}
