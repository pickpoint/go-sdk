package tracking_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestPublishRateLimit(t *testing.T) {
	ms := startMock(t, true, nil)
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         ms.URL,
		Device:           &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if _, err := c.StartTrack(ctx, &tracking.LatLng{Latitude: 1, Longitude: 2}, nil); err != nil {
		t.Fatal(err)
	}

	accepted := 0
	for i := 0; i < tracking.MaxPublishHz*3; i++ {
		_, ok := c.Publish(&tracking.LatLng{Latitude: float64(i), Longitude: 0})
		if ok {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d want 1 in same burst", accepted)
	}
	if c.ClientSeq() != 1 {
		t.Fatalf("seq=%d want 1", c.ClientSeq())
	}

	time.Sleep(tracking.MinPublishInterval + 5*time.Millisecond)
	seq, ok := c.Publish(&tracking.LatLng{Latitude: 9, Longitude: 9})
	if !ok || seq != 2 {
		t.Fatalf("ok=%v seq=%d", ok, seq)
	}
}

func TestSendEventLimits(t *testing.T) {
	ms := startMock(t, true, nil)
	defer ms.close()
	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         ms.URL,
		Device:           &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.StartTrack(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := c.SendEvent(make([]byte, tracking.MaxEventBytes+1)); err == nil {
		t.Fatal("expected oversized error")
	}
	ok, err := c.SendEvent([]byte("a"))
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	ok, err = c.SendEvent([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second event should be rate-limited")
	}
}

func TestResumeAfterPublish(t *testing.T) {
	ms := startMock(t, true, nil)
	defer ms.close()
	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         ms.URL,
		Device:           &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	uid, err := c.StartTrack(ctx, &tracking.LatLng{Latitude: 1, Longitude: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Publish(&tracking.LatLng{Latitude: 2, Longitude: 2}); !ok {
		t.Fatal("publish")
	}
	waitFor(t, func() bool { return c.LastAckedSeq() == 1 }, 2*time.Second)

	acked, err := c.Resume(ctx, uid, 1)
	if err != nil {
		t.Fatal(err)
	}
	if acked != 0 {
		t.Fatalf("acked=%d", acked)
	}
	ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Resume != nil
	}, 2*time.Second)
}

func TestListenerSubscribeAndLocation(t *testing.T) {
	ms := startMock(t, true, func(msg tracking.ClientMsg, c *mockConn) {
		if msg.Subscribe != nil {
			go func() {
				time.Sleep(20 * time.Millisecond)
				_ = c.send(tracking.ServerMsg{Loc: &tracking.ServerLoc{
					Sub:   1,
					Seq:   3,
					Point: tracking.LatLng{Latitude: 1.5, Longitude: 2.5},
				}})
			}()
		}
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         ms.URL,
		Listener:         &tracking.ListenerAuth{AccessToken: "jwt"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Subscribe(mockDeviceUID); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := c.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Loc != nil {
			if msg.Loc.Point.Latitude != 1.5 {
				t.Fatalf("%+v", msg.Loc)
			}
			return
		}
	}
	t.Fatal("no location")
}

func TestUnsubscribeByHandle(t *testing.T) {
	ms := startMock(t, true, nil)
	defer ms.close()
	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:         ms.URL,
		Listener:         &tracking.ListenerAuth{AccessToken: "jwt"},
		DisableReconnect: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Subscribe(mockDeviceUID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var handle uint8
	for time.Now().Before(deadline) {
		msg, err := c.Recv(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Subscribed != nil {
			handle = msg.Subscribed.Sub
			break
		}
	}
	if handle == 0 {
		t.Fatal("no subscribed handle")
	}
	if err := c.Unsubscribe(handle); err != nil {
		t.Fatal(err)
	}
	un := ms.waitMsg(t, func(m tracking.ClientMsg) bool {
		return m.Unsubscribe != nil && m.Unsubscribe.Sub == handle
	}, 2*time.Second)
	if un.Unsubscribe.Sub != handle {
		t.Fatalf("%+v", un.Unsubscribe)
	}
}

func TestAuthErrorWithoutRefreshCloses(t *testing.T) {
	ms := startMock(t, false, func(msg tracking.ClientMsg, c *mockConn) {
		if msg.TrackStart != nil {
			_ = c.send(serverError(tracking.ErrorAuth, "bad creds"))
		}
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 10 * time.Millisecond,
		ReconnectMaxDelay: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.StartTrack(ctx, nil, nil)
	var te *tracking.Error
	if !errors.As(err, &te) || te.Code != tracking.ErrorAuth {
		t.Fatalf("%v", err)
	}
	waitFor(t, func() bool { return c.State() == tracking.StateClosed }, 3*time.Second)
}

func TestAuthErrorRefreshRedials(t *testing.T) {
	var hellos atomic.Int32
	refreshed := make(chan struct{}, 1)

	ms := startMockOpts(t, mockOpts{
		auto: false,
		beforeHello: func(int, *mockConn) {
			hellos.Add(1)
		},
		onMsg: func(msg tracking.ClientMsg, c *mockConn) {
			if msg.TrackStart != nil {
				_ = c.send(serverError(tracking.ErrorUnauthorized, "expired"))
			}
		},
	})
	defer ms.close()

	ctx := context.Background()
	c, err := tracking.Connect(ctx, tracking.Config{
		Endpoint:          ms.URL,
		Device:            &tracking.DeviceAuth{ClientID: "c", ClientSecret: "s"},
		ReconnectMinDelay: 15 * time.Millisecond,
		ReconnectMaxDelay: 40 * time.Millisecond,
		HelloTimeout:      2 * time.Second,
		RefreshAuth: func(context.Context) (*tracking.DeviceAuth, *tracking.ListenerAuth, error) {
			select {
			case refreshed <- struct{}{}:
			default:
			}
			return &tracking.DeviceAuth{ClientID: "c2", ClientSecret: "s2"}, nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	go func() { _, _ = c.StartTrack(ctx, nil, nil) }()

	select {
	case <-refreshed:
	case <-time.After(3 * time.Second):
		t.Fatal("refreshAuth not called")
	}
	waitFor(t, func() bool { return hellos.Load() >= 2 }, 5*time.Second)
}
