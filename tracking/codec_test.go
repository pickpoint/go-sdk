package tracking_test

import (
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
	pb "github.com/pickpoint/go-sdk/tracking/v2"
	"google.golang.org/protobuf/proto"
)

func TestStampLatLngDefaultTimestamp(t *testing.T) {
	before := time.Now().UnixMilli()
	p := tracking.StampLatLng(&pb.LatLng{Latitude: 1, Longitude: 2})
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
	p := tracking.StampLatLng(&pb.LatLng{Latitude: 1, Longitude: 2, TimestampMs: &ts})
	if p.GetTimestampMs() != 42 {
		t.Fatalf("%d", p.GetTimestampMs())
	}
}

func TestCodecRoundTripResume(t *testing.T) {
	msg := tracking.ClientResume("t1", 9)
	b, err := tracking.EncodeClientMsg(msg)
	if err != nil {
		t.Fatal(err)
	}
	var round pb.ClientMsg
	if err := proto.Unmarshal(b, &round); err != nil {
		t.Fatal(err)
	}
	if round.GetResume().GetTrackUid() != "t1" || round.GetResume().GetLastClientSeq() != 9 {
		t.Fatalf("%v", round.GetResume())
	}
}

func TestCodecRoundTripHello(t *testing.T) {
	msg := &pb.ServerMsg{Body: &pb.ServerMsg_Hello{Hello: &pb.Hello{NodeId: "n1", Shard: 7}}}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tracking.DecodeServerMsg(b)
	if err != nil {
		t.Fatal(err)
	}
	h := got.GetHello()
	if h.GetNodeId() != "n1" || h.GetShard() != 7 {
		t.Fatalf("%v", h)
	}
}
