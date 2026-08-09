package pickpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestDevices404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"message":"Device not found"}`))
	}))
	defer srv.Close()
	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL})
	_, err := c.Devices.Get(context.Background(), "missing")
	if !errors.Is(err, pickpoint.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestDevicesConflict409(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(409)
		_, _ = w.Write([]byte(`{"message":"device offline"}`))
	}))
	defer srv.Close()
	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL})
	_, err := c.Devices.Command(context.Background(), "u1", []byte("x"))
	if !errors.Is(err, pickpoint.ErrConflict) {
		t.Fatalf("%v", err)
	}
}

func TestCommandBase64(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload string `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Payload != "aGk=" { // "hi"
			t.Fatalf("payload %q", body.Payload)
		}
		_, _ = w.Write([]byte(`{"delivered":1}`))
	}))
	defer srv.Close()
	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL})
	out, err := c.Devices.Command(context.Background(), "uid-1", []byte("hi"))
	if err != nil || out.Delivered != 1 {
		t.Fatalf("%v %#v", err, out)
	}
}
