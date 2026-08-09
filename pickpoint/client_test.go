package pickpoint_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestInvalidConfig(t *testing.T) {
	_, err := pickpoint.New(pickpoint.Config{})
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = pickpoint.New(pickpoint.Config{APIKey: "a", AccessToken: "b"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestForwardAndSearchShareAPIKey(t *testing.T) {
	var sawKey int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "secret" {
			t.Errorf("missing api key on %s", r.URL.Path)
		}
		atomic.AddInt32(&sawKey, 1)
		switch {
		case strings.Contains(r.URL.Path, "/geocode/forward"):
			_, _ = w.Write([]byte(`[{"display_name":"Berlin"}]`))
		case strings.Contains(r.URL.Path, "/address/search"):
			_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
		case r.URL.Path == "/v2/devices":
			_, _ = w.Write([]byte(`{"data":[{"uid":"d1","name":"A","type":"car"}],"total":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, err := pickpoint.New(pickpoint.Config{
		APIKey:  "secret",
		BaseURL: srv.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	places, err := c.Forward(ctx, pickpoint.Query{"q": "Berlin"})
	if err != nil || len(places) != 1 {
		t.Fatalf("forward: %v %#v", err, places)
	}
	if _, err := c.Search(ctx, pickpoint.Query{"q": "Berlin"}); err != nil {
		t.Fatal(err)
	}
	list, err := c.Devices.List(ctx, pickpoint.DeviceListQuery{})
	if err != nil || list.Total != 1 {
		t.Fatalf("devices: %v %#v", err, list)
	}
	if atomic.LoadInt32(&sawKey) != 3 {
		t.Fatalf("expected 3 authed calls, got %d", sawKey)
	}
}
