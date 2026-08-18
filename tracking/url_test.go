package tracking_test

import (
	"testing"

	"github.com/pickpoint/go-sdk/tracking"
)

func TestBuildWSURLDevice(t *testing.T) {
	u, err := tracking.BuildWSURL(tracking.Config{
		Endpoint: "https://tracking.example.com",
		Device:   &tracking.DeviceAuth{ClientID: "id", ClientSecret: "sec"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "wss" || u.Path != "/v2/ws" {
		t.Fatalf("%s", u)
	}
	q := u.Query()
	if q.Get("client-id") != "id" || q.Get("client-secret") != "sec" {
		t.Fatalf("%v", q)
	}
}

func TestBuildWSURLListener(t *testing.T) {
	u, err := tracking.BuildWSURL(tracking.Config{
		Endpoint: "ws://localhost:1",
		Listener: &tracking.ListenerAuth{AccessToken: "jwt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("access-token") != "jwt" {
		t.Fatalf("%v", u.Query())
	}
}
