package tracking_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
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
	c.Publish(&tracking.LatLng{Latitude: 1, Longitude: 2})
	time.Sleep(25 * time.Millisecond)
	c.Publish(&tracking.LatLng{Latitude: 3, Longitude: 4})
	if c.ClientSeq() != 2 {
		t.Fatalf("seq=%d", c.ClientSeq())
	}

	first := ms.waitConn(t, 2*time.Second)
	first.close()

	resume := ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Resume != nil
	}, 8*time.Second)
	r := resume.Resume
	if r.TrackUID != uid || r.LastSeq != 2 {
		t.Fatalf("%+v", r)
	}

	var starts int
	ms.mu.Lock()
	for _, conn := range ms.connections {
		conn.mu.Lock()
		for _, m := range conn.messages {
			if m.TrackStart != nil {
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
	ms := startMock(t, false, func(msg tracking.ClientMsg, c *mockConn) {
		switch {
		case msg.TrackStart != nil:
			_ = c.send(tracking.ServerMsg{TrackStarted: &tracking.TrackStarted{TrackUID: mockTrackUID}})
		case msg.Resume != nil:
			_ = c.send(serverError(tracking.ErrorTrackNotFound, "track expired"))
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
	if c.TrackUID() != mockTrackUID {
		t.Fatalf("%q", c.TrackUID())
	}

	conn := ms.waitConn(t, 2*time.Second)
	conn.close()

	ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Resume != nil
	}, 8*time.Second)

	waitFor(t, func() bool { return c.TrackUID() == "" }, 5*time.Second)
}

func TestFencedResumeRetriesNotFatal(t *testing.T) {
	var resumes atomic.Int32
	ms := startMock(t, false, func(msg tracking.ClientMsg, c *mockConn) {
		switch {
		case msg.TrackStart != nil:
			_ = c.send(tracking.ServerMsg{TrackStarted: &tracking.TrackStarted{TrackUID: mockTrackUID}})
		case msg.Resume != nil:
			n := resumes.Add(1)
			if n == 1 {
				_ = c.send(serverError(tracking.ErrorFenced, "draining"))
			} else {
				_ = c.send(tracking.ServerMsg{ResumeOk: &tracking.ResumeOk{
					TrackUID:  mockTrackUID,
					LastAcked: 0,
				}})
			}
		}
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 15 * time.Millisecond,
		ReconnectMaxDelay: 30 * time.Millisecond,
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

	waitFor(t, func() bool { return resumes.Load() >= 2 && c.TrackUID() == mockTrackUID }, 8*time.Second)
}

func TestRelocateDialsNewEndpoint(t *testing.T) {
	target := startMock(t, true, nil)
	defer target.close()

	gateway := startMockOpts(t, mockOpts{
		auto: false,
		relocateOnConnect: &tracking.Relocate{
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
	if uid != mockTrackUID {
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
	seq, ok := c.Publish(&tracking.LatLng{Latitude: 9, Longitude: 9})
	if !ok {
		t.Fatal("publish while reconnecting should stage")
	}
	if seq != 0 {
		t.Fatalf("seq=%d want 0 (assigned after filter+flush)", seq)
	}
	release.Done()

	ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Resume != nil
	}, 8*time.Second)
	ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Loc != nil
	}, 8*time.Second)
	waitFor(t, func() bool { return c.ClientSeq() == 1 }, 3*time.Second)
}
