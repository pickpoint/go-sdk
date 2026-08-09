package pickpoint_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestContextCancelAbortsRequest(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL, Timeout: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	_, err := c.Search(ctx, pickpoint.Query{"q": "x"})
	if err == nil {
		t.Fatal("expected error")
	}
}
