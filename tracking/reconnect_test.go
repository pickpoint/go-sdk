package tracking_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

func TestReconnectSendsResumeNotTrackStart(t *testing.T) {
	ms := startMock(t, true, nil)
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 20 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	uid, err := c.StartTrack(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	c.Publish(&pb.LatLng{Latitude: 1, Longitude: 2})
	time.Sleep(25 * time.Millisecond)
	c.Publish(&pb.LatLng{Latitude: 3, Longitude: 4})
	if c.ClientSeq() != 2 {
		t.Fatalf("seq=%d", c.ClientSeq())
	}

	first := ms.waitConn(t, 2*time.Second)
	first.close()

	resume := ms.waitMsg(t, func(m *pb.ClientMsg) bool {
		_, ok := m.Body.(*pb.ClientMsg_Resume)
		return ok
	}, 8*time.Second)
	r := resume.GetResume()
	if r.GetTrackUid() != uid || r.GetLastClientSeq() != 2 {
		t.Fatalf("%v", r)
	}

	// Must not have started a new track on reconnect
	var starts int
	ms.mu.Lock()
	for _, conn := range ms.connections {
		conn.mu.Lock()
		for _, m := range conn.messages {
			if _, ok := m.Body.(*pb.ClientMsg_TrackStart); ok {
				starts++
			}
		}
		conn.mu.Unlock()
	}
	ms.mu.Unlock()
	if starts != 1 {
		t.Fatalf("trackStarts=%d", starts)
	}

	waitFor(t, func() bool { return c.State() == tracking.StateOpen }, 5*time.Second)
}

func TestReconnectTrackNotFoundClearsCursor(t *testing.T) {
	ms := startMock(t, false, func(msg *pb.ClientMsg, c *mockConn) {
		switch msg.Body.(type) {
		case *pb.ClientMsg_TrackStart:
			_ = c.send(&pb.ServerMsg{Body: &pb.ServerMsg_TrackStarted{TrackStarted: &pb.TrackStarted{TrackUid: "t-gone"}}})
		case *pb.ClientMsg_Resume:
			_ = c.send(serverError(pb.ErrorCode_ERROR_CODE_TRACK_NOT_FOUND, "track expired"))
		}
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 20 * time.Millisecond,
		ReconnectMaxDelay: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.StartTrack(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if c.TrackUID() != "t-gone" {
		t.Fatalf("%q", c.TrackUID())
	}

	conn := ms.waitConn(t, 2*time.Second)
	conn.close()

	ms.waitMsg(t, func(m *pb.ClientMsg) bool {
		_, ok := m.Body.(*pb.ClientMsg_Resume)
		return ok
	}, 8*time.Second)

	waitFor(t, func() bool { return c.TrackUID() == "" }, 5*time.Second)
}

func TestRelocateDialsNewEndpoint(t *testing.T) {
	target := startMock(t, true, nil)
	defer target.close()

	gateway := startMockOpts(t, mockOpts{
		auto: false,
		relocateOnConnect: &pb.Relocate{
			Endpoint:     target.URL,
			RetryAfterMs: 10,
		},
	})
	defer gateway.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         gateway.URL,
		Device:           &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if c.State() != tracking.StateOpen {
		t.Fatalf("state %s", c.State())
	}
	if target.connCount() < 1 {
		t.Fatal("expected connection on target")
	}
	uid, err := c.StartTrack(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if uid != "track-mock-1" {
		t.Fatalf("%q", uid)
	}
}

func TestQueueFlushAfterResume(t *testing.T) {
	var release sync.WaitGroup
	release.Add(1)

	ms := startMockOpts(t, mockOpts{
		auto: true,
		beforeHello: func(idx int, _ *mockConn) {
			if idx >= 2 {
				release.Wait()
			}
		},
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 20 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.StartTrack(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	conn := ms.waitConn(t, 2*time.Second)
	conn.close()

	waitFor(t, func() bool { return c.State() == tracking.StateReconnecting }, 3*time.Second)
	seq, ok := c.Publish(&pb.LatLng{Latitude: 9, Longitude: 9})
	if !ok || seq != 1 {
		t.Fatalf("seq=%d ok=%v", seq, ok)
	}
	release.Done()

	ms.waitMsg(t, func(m *pb.ClientMsg) bool {
		_, ok := m.Body.(*pb.ClientMsg_Resume)
		return ok
	}, 8*time.Second)
	ms.waitMsg(t, func(m *pb.ClientMsg) bool {
		_, batch := m.Body.(*pb.ClientMsg_LocationBatch)
		_, add := m.Body.(*pb.ClientMsg_LocationAdd)
		return batch || add
	}, 8*time.Second)
}
