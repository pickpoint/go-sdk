package tracking

// ProtocolVersion is Hello.version. The WS subprotocol is [Subprotocol].
const ProtocolVersion = 2

// Subprotocol is the WebSocket subprotocol for tracking.v2.
const Subprotocol = "tracking.v2"

// Wire type bytes (client 0x01–0x7E, server 0x80–0xFE).
const (
	TypeResume      uint8 = 0x01
	TypeTrackStart  uint8 = 0x02
	TypeTrackStop   uint8 = 0x03
	TypeLoc         uint8 = 0x04
	TypeSubscribe   uint8 = 0x05
	TypeUnsubscribe uint8 = 0x06
	TypeEvent       uint8 = 0x07
	TypeCommandAck  uint8 = 0x08

	TypeHello        uint8 = 0x80
	TypeRelocate     uint8 = 0x81
	TypeResumeOk     uint8 = 0x82
	TypeTrackStarted uint8 = 0x83
	TypeTrackStopped uint8 = 0x84
	TypeAck          uint8 = 0x85
	TypeServerLoc    uint8 = 0x86
	TypeSubscribed   uint8 = 0x87
	TypeError        uint8 = 0x88
	TypeEventAdded   uint8 = 0x89
	TypeCommand      uint8 = 0x8A
	TypePresence     uint8 = 0x8B
)

const (
	MaxStringBytes    = 4096
	MaxLocPoints      = 100
	MaxBufferPoints   = 10_000
	MaxInFlightFrames = 8
)

// ErrorCode is wire Error.code (u8). 0 is unused.
type ErrorCode uint8

const (
	ErrorUnspecified   ErrorCode = 0
	ErrorAuth          ErrorCode = 1
	ErrorTrackNotFound ErrorCode = 2
	ErrorFenced        ErrorCode = 3
	ErrorTryAgain      ErrorCode = 4
	ErrorInvalid       ErrorCode = 5
	ErrorUnauthorized  ErrorCode = 6
)

func (c ErrorCode) String() string {
	switch c {
	case ErrorAuth:
		return "AUTH"
	case ErrorTrackNotFound:
		return "TRACK_NOT_FOUND"
	case ErrorFenced:
		return "FENCED"
	case ErrorTryAgain:
		return "TRY_AGAIN"
	case ErrorInvalid:
		return "INVALID"
	case ErrorUnauthorized:
		return "UNAUTHORIZED"
	default:
		return "UNSPECIFIED"
	}
}

func errorCodeFromU8(v uint8) (ErrorCode, bool) {
	if v >= 1 && v <= 6 {
		return ErrorCode(v), true
	}
	return 0, false
}

// CommandAckStatus is wire CommandAck.status (u8).
type CommandAckStatus uint8

const (
	CommandAckUnspecified CommandAckStatus = 0
	CommandAckOK          CommandAckStatus = 1
	CommandAckRejected    CommandAckStatus = 2
	CommandAckFailed      CommandAckStatus = 3
)

func commandAckFromU8(v uint8) CommandAckStatus {
	switch v {
	case 1:
		return CommandAckOK
	case 2:
		return CommandAckRejected
	case 3:
		return CommandAckFailed
	default:
		return CommandAckUnspecified
	}
}

// LatLng is a WGS-84 point. Heading and Speed are filter-only (not on the wire).
type LatLng struct {
	Latitude    float64
	Longitude   float64
	Altitude    *float64 // metres
	Accuracy    *float64 // metres
	TimestampMs *int64   // unix ms
	Heading     *float64 // degrees, GPS chip, filter only
	Speed       *float64 // m/s, filter only
}

// ClientMsg is one client→server frame (exactly one field set).
type ClientMsg struct {
	Resume      *Resume
	TrackStart  *TrackStart
	TrackStop   *TrackStop
	Loc         *Loc
	Subscribe   *Subscribe
	Unsubscribe *Unsubscribe
	Event       *Event
	CommandAck  *CommandAck
	// Unknown is set when decoding a reserved-range client type the SDK does not implement.
	Unknown *uint8
}

// ServerMsg is one server→client frame (exactly one field set).
// Unknown server types decode as a zero value (ignore, forward-compatible).
type ServerMsg struct {
	Hello        *Hello
	Relocate     *Relocate
	ResumeOk     *ResumeOk
	TrackStarted *TrackStarted
	TrackStopped *TrackStopped
	Ack          *Ack
	Loc          *ServerLoc
	Subscribed   *Subscribed
	Error        *WireError
	EventAdded   *EventAdded
	Command      *Command
	Presence     *Presence
}

type Resume struct {
	TrackUID string
	LastSeq  uint32
}

type TrackStart struct {
	Location *LatLng
	Route    []LatLng
	Metadata []byte
}

type TrackStop struct{}

// Loc is client 0x04: seq is the last point in Points.
type Loc struct {
	Seq    uint32
	Points []LatLng
}

type Subscribe struct {
	DeviceUID     string
	IncludeEvents bool
	MinIntervalMs uint16
}

type Unsubscribe struct {
	Sub uint8
}

type Event struct {
	Payload     []byte
	TimestampMs int64 // 0 = absent
}

type CommandAck struct {
	CommandID string
	Status    CommandAckStatus
	Message   string
}

type Hello struct {
	Version uint8
	Shard   uint16
	NodeID  string
}

type Relocate struct {
	RetryAfterMs uint32
	Endpoint     string
}

type ResumeOk struct {
	TrackUID  string
	LastAcked uint32
}

type TrackStarted struct {
	TrackUID string
	Metadata []byte
}

type TrackStopped struct {
	TrackUID string
}

// Ack is device-only 0x85 (ingest receipt). Not a live map event.
type Ack struct {
	Seq uint32
}

// ServerLoc is listener-only 0x86 (one absolute point).
type ServerLoc struct {
	Sub   uint8
	Seq   uint32
	Point LatLng
}

type Subscribed struct {
	Sub          uint8
	DeviceUID    string
	TrackUID     string // empty = no live track
	Online       bool
	LastLocation *LatLng
	LastSeenMs   *int64
	Route        []LatLng
	EstDistance  float64
	EstDuration  float64
	StartName    string
	EndName      string
	Metadata     []byte
}

type WireError struct {
	Code         ErrorCode
	RetryAfterMs uint32 // 0 = none
	TrackUID     string // empty = none
	Message      string
}

type EventAdded struct {
	Sub         uint8
	Payload     []byte
	TimestampMs int64
}

type Command struct {
	CommandID   string
	Payload     []byte
	TimestampMs int64
}

type Presence struct {
	Sub        uint8
	Online     bool
	LastSeenMs int64 // 0 = none
}
