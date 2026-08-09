package tracking

import (
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

// StampLatLng sets TimestampMs to now when the caller omitted it.
func StampLatLng(p *pb.LatLng) *pb.LatLng {
	return stampLatLng(p)
}

func stampLatLng(p *pb.LatLng) *pb.LatLng {
	if p == nil {
		return nil
	}
	if p.TimestampMs == nil {
		now := time.Now().UnixMilli()
		p.TimestampMs = &now
	}
	return p
}

func stampLatLngs(points []*pb.LatLng) []*pb.LatLng {
	for _, p := range points {
		stampLatLng(p)
	}
	return points
}

// EncodeClientMsg marshals a ClientMsg to binary protobuf.
func EncodeClientMsg(msg *pb.ClientMsg) ([]byte, error) {
	return proto.Marshal(msg)
}

// DecodeServerMsg unmarshals a ServerMsg from binary protobuf.
func DecodeServerMsg(data []byte) (*pb.ServerMsg, error) {
	var msg pb.ServerMsg
	if err := proto.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// ClientResume builds a resume ClientMsg (for golden / wire tests).
func ClientResume(trackUID string, lastClientSeq uint64) *pb.ClientMsg {
	return &pb.ClientMsg{Body: &pb.ClientMsg_Resume{Resume: &pb.Resume{
		TrackUid: trackUID, LastClientSeq: lastClientSeq,
	}}}
}
