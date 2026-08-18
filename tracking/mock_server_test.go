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
)

const (
	mockTrackUID  = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	mockDeviceUID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	mockNodeID    = "cccccccc-cccc-cccc-cccc-cccccccccccc"
)

type mockConn struct {
	mu       sync.Mutex
	messages []tracking.ClientMsg
	ws       *websocket.Conn
}

func (c *mockConn) send(msg tracking.ServerMsg) error {
	b, err := tracking.EncodeServerMsg(msg)
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
	onMsg             func(msg tracking.ClientMsg, c *mockConn)
	beforeHello       func(connectionIndex int, c *mockConn)
	relocateOnConnect *tracking.Relocate
}

type mockServer struct {
	URL         string
	server      *httptest.Server
	mu          sync.Mutex
	connections []*mockConn
	opts        mockOpts
}

func startMock(t *testing.T, auto bool, onMsg func(tracking.ClientMsg, *mockConn)) *mockServer {
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
	nextSub := uint8(1)
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
				_ = c.send(tracking.ServerMsg{Relocate: opts.relocateOnConnect})
			} else {
				_ = c.send(tracking.ServerMsg{Hello: &tracking.Hello{
					Version: tracking.ProtocolVersion,
					NodeID:  mockNodeID,
				}})
			}

			for {
				_, data, err := ws.ReadMessage()
				if err != nil {
					return
				}
				msg, err := tracking.DecodeClientMsg(data)
				if err != nil {
					continue
				}
				c.mu.Lock()
				c.messages = append(c.messages, msg)
				c.mu.Unlock()
				if opts.onMsg != nil {
					opts.onMsg(msg, c)
				}
				if !opts.auto {
					continue
				}
				switch {
				case msg.TrackStart != nil:
					_ = c.send(tracking.ServerMsg{TrackStarted: &tracking.TrackStarted{TrackUID: mockTrackUID}})
				case msg.TrackStop != nil:
					_ = c.send(tracking.ServerMsg{TrackStopped: &tracking.TrackStopped{TrackUID: mockTrackUID}})
				case msg.Resume != nil:
					_ = c.send(tracking.ServerMsg{ResumeOk: &tracking.ResumeOk{
						TrackUID:  msg.Resume.TrackUID,
						LastAcked: 0,
					}})
				case msg.Loc != nil:
					_ = c.send(tracking.ServerMsg{Ack: &tracking.Ack{Seq: msg.Loc.Seq}})
				case msg.Subscribe != nil:
					sub := nextSub
					nextSub++
					_ = c.send(tracking.ServerMsg{Subscribed: &tracking.Subscribed{
						Sub:       sub,
						DeviceUID: msg.Subscribe.DeviceUID,
						TrackUID:  mockTrackUID,
						Online:    true,
					}})
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

func (ms *mockServer) waitMsg(t *testing.T, pred func(tracking.ClientMsg) bool, timeout time.Duration) tracking.ClientMsg {
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
	return tracking.ClientMsg{}
}

func serverError(code tracking.ErrorCode, message string) tracking.ServerMsg {
	return tracking.ServerMsg{Error: &tracking.WireError{
		Code: code, Message: message,
	}}
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
