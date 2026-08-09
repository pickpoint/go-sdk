package tracking_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pickpoint/go-sdk/tracking"
	pb "github.com/pickpoint/go-sdk/tracking/v2"
	"google.golang.org/protobuf/proto"
)

type mockConn struct {
	mu       sync.Mutex
	messages []*pb.ClientMsg
	ws       *websocket.Conn
}

func (c *mockConn) send(msg *pb.ServerMsg) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return c.ws.WriteMessage(websocket.BinaryMessage, b)
}

func (c *mockConn) close() {
	_ = c.ws.Close()
}

type mockOpts struct {
	auto              bool
	onMsg             func(msg *pb.ClientMsg, c *mockConn)
	beforeHello       func(connectionIndex int, c *mockConn)
	relocateOnConnect *pb.Relocate
}

type mockServer struct {
	URL         string
	server      *httptest.Server
	mu          sync.Mutex
	connections []*mockConn
	opts        mockOpts
}

func startMock(t *testing.T, auto bool, onMsg func(*pb.ClientMsg, *mockConn)) *mockServer {
	t.Helper()
	return startMockOpts(t, mockOpts{auto: auto, onMsg: onMsg})
}

func startMockOpts(t *testing.T, opts mockOpts) *mockServer {
	t.Helper()
	up := websocket.Upgrader{
		CheckOrigin:  func(r *http.Request) bool { return true },
		Subprotocols: []string{tracking.Subprotocol},
	}
	ms := &mockServer{opts: opts}
	ms.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, http.Header{
			"Sec-WebSocket-Protocol": []string{tracking.Subprotocol},
		})
		if err != nil {
			return
		}
		c := &mockConn{ws: ws}
		ms.mu.Lock()
		ms.connections = append(ms.connections, c)
		idx := len(ms.connections)
		ms.mu.Unlock()

		go func() {
			if opts.beforeHello != nil {
				opts.beforeHello(idx, c)
			}
			if opts.relocateOnConnect != nil && idx == 1 {
				_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_Relocate{Relocate: opts.relocateOnConnect}})
			} else {
				_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_Hello{Hello: &pb.Hello{NodeId: "mock-1"}}})
			}

			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					return
				}
				var msg pb.ClientMsg
				if err := proto.Unmarshal(data, &msg); err != nil {
					continue
				}
				c.mu.Lock()
				c.messages = append(c.messages, proto.Clone(&msg).(*pb.ClientMsg))
				c.mu.Unlock()
				if opts.onMsg != nil {
					opts.onMsg(&msg, c)
				}
				if !opts.auto {
					continue
				}
				switch b := msg.Body.(type) {
				case *pb.ClientMsg_TrackStart:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_TrackStarted{TrackStarted: &pb.TrackStarted{TrackUid: "track-mock-1"}}})
				case *pb.ClientMsg_TrackStop:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_TrackStopped{TrackStopped: &pb.TrackStopped{TrackUid: b.TrackStop.GetTrackUid()}}})
				case *pb.ClientMsg_Resume:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_ResumeOk{ResumeOk: &pb.ResumeOk{
						TrackUid: b.Resume.GetTrackUid(), LastAckedSeq: 0,
					}}})
				case *pb.ClientMsg_LocationAdd:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_LocationAdded{LocationAdded: &pb.LocationAdded{
						TrackUid: b.LocationAdd.GetTrackUid(), ClientSeq: b.LocationAdd.GetClientSeq(),
						Point: b.LocationAdd.GetPoint(), DeviceUid: "dev-1",
					}}})
				case *pb.ClientMsg_LocationBatch:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_LocationAdded{LocationAdded: &pb.LocationAdded{
						TrackUid: b.LocationBatch.GetTrackUid(), ClientSeq: b.LocationBatch.GetClientSeq(),
						DeviceUid: "dev-1",
					}}})
				case *pb.ClientMsg_Subscribe:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_Subscribed{Subscribed: &pb.Subscribed{
						DeviceUid: b.Subscribe.GetDeviceUid(), TrackUid: "track-mock-1",
					}}})
				case *pb.ClientMsg_Ping:
					_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_Pong{Pong: &pb.Pong{}}})
				}
			}
		}()
	}))
	ms.URL = "ws" + strings.TrimPrefix(ms.server.URL, "http")
	return ms
}

func (ms *mockServer) close() {
	ms.server.Close()
}

func (ms *mockServer) waitConn(t *testing.T, timeout time.Duration) *mockConn {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ms.mu.Lock()
		if len(ms.connections) > 0 {
			c := ms.connections[0]
			ms.mu.Unlock()
			return c
		}
		ms.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitConn timeout")
	return nil
}

func (ms *mockServer) connCount() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.connections)
}

func (ms *mockServer) waitMsg(t *testing.T, pred func(*pb.ClientMsg) bool, timeout time.Duration) *pb.ClientMsg {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ms.mu.Lock()
		for _, c := range ms.connections {
			c.mu.Lock()
			for _, m := range c.messages {
				if pred(m) {
					c.mu.Unlock()
					ms.mu.Unlock()
					return m
				}
			}
			c.mu.Unlock()
		}
		ms.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitMsg timeout")
	return nil
}

func serverError(code pb.ErrorCode, message string) *pb.ServerMsg {
	return &pb.ServerMsg{Body: &pb.ServerMsg_Error{Error: &pb.Error{
		Code: code, Message: message,
	}}}
}

func waitFor(t *testing.T, pred func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("waitFor timeout")
}
