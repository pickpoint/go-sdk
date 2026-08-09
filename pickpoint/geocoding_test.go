package pickpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestGeocodeEmptyOn400(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"message":"bad"}`))
	}))
	defer srv.Close()
	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL})
	out, err := c.Forward(context.Background(), pickpoint.Query{"q": "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("want empty, got %#v", out)
	}
}

func TestForwardBatch(t *testing.T) {
	var inflight atomic.Int32
	var max atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inflight.Add(1)
		for {
			old := max.Load()
			if cur <= old || max.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		_, _ = w.Write([]byte(`[{"ok":true}]`))
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		APIKey:      "k",
		BaseURL:     srv.URL,
		Concurrency: 4,
	})
	qs := make([]pickpoint.Query, 12)
	for i := range qs {
		qs[i] = pickpoint.Query{"q": "x"}
	}
	out, err := c.ForwardBatch(context.Background(), qs)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 12 {
		t.Fatalf("len %d", len(out))
	}
	if max.Load() > 4 {
		t.Fatalf("concurrency leaked: %d", max.Load())
	}
}

// Pipeline (not wave): with concurrency 2, a slow first slot must not block
// later fast slots from starting — the third request begins while the slow one
// is still in flight.
func TestBatchPipelineFillsSlots(t *testing.T) {
	var mu sync.Mutex
	started := map[string]time.Time{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		mu.Lock()
		started[q] = time.Now()
		mu.Unlock()
		if q == "slow" {
			time.Sleep(80 * time.Millisecond)
		} else {
			time.Sleep(5 * time.Millisecond)
		}
		_, _ = w.Write([]byte(`[{"ok":true}]`))
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		APIKey:      "k",
		BaseURL:     srv.URL,
		Concurrency: 2,
	})
	_, err := c.ForwardBatch(context.Background(), []pickpoint.Query{
		{"q": "slow"}, {"q": "a"}, {"q": "b"}, {"q": "c"},
	})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	slowAt, aAt, bAt := started["slow"], started["a"], started["b"]
	if slowAt.IsZero() || aAt.IsZero() || bAt.IsZero() {
		t.Fatalf("missing starts: %#v", started)
	}
	// Wave/chunk semantics would start "b" only after both "slow" and "a" finish (~80ms).
	// Pipeline starts "b" as soon as "a" frees a slot (~5ms), while "slow" still runs.
	if bAt.Sub(aAt) > 40*time.Millisecond {
		t.Fatalf("b started too late after a (%v) — looks like wave batching, not a conveyor", bAt.Sub(aAt))
	}
	if !bAt.Before(slowAt.Add(60 * time.Millisecond)) {
		t.Fatalf("b should overlap slow; bAt=%v slowAt=%v", bAt, slowAt)
	}
}

func TestBatchAbortOn403(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if strings.Contains(r.URL.RawQuery, "q=bad") {
			w.WriteHeader(403)
			return
		}
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte(`[{"ok":true}]`))
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		APIKey:      "k",
		BaseURL:     srv.URL,
		Concurrency: 4,
	})
	_, err := c.ForwardBatch(context.Background(), []pickpoint.Query{
		{"q": "bad"}, {"q": "a"}, {"q": "b"}, {"q": "c"}, {"q": "d"}, {"q": "e"},
	})
	if !errors.Is(err, pickpoint.ErrAuth) {
		t.Fatalf("%v", err)
	}
	if hits.Load() >= 6 {
		t.Fatalf("expected abort before all slots, hits=%d", hits.Load())
	}
}

func TestBatchPreservesOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "q=slow") {
			time.Sleep(60 * time.Millisecond)
			_, _ = w.Write([]byte(`[{"id":"slow"}]`))
			return
		}
		_, _ = w.Write([]byte(`[{"id":"fast"}]`))
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL, Concurrency: 10})
	out, err := c.ForwardBatch(context.Background(), []pickpoint.Query{
		{"q": "slow"}, {"q": "fast1"}, {"q": "fast2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out[0][0].(map[string]any)["id"] != "slow" {
		t.Fatalf("%#v", out[0])
	}
	if out[1][0].(map[string]any)["id"] != "fast" {
		t.Fatalf("%#v", out[1])
	}
}

func TestRetryBudgetPerSlot(t *testing.T) {
	attempts := map[string]int{}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		mu.Lock()
		attempts[q]++
		n := attempts[q]
		mu.Unlock()
		if q == "flaky" && n < 3 {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]string{{"q": q}})
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		APIKey:     "k",
		BaseURL:    srv.URL,
		MaxRetries: 5,
		RetryBase:  pickpoint.MinRetryBase,
	})
	out, err := c.ForwardBatch(context.Background(), []pickpoint.Query{
		{"q": "flaky"}, {"q": "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts["flaky"] != 3 || attempts["ok"] != 1 {
		t.Fatalf("%v", attempts)
	}
	if out[0][0].(map[string]any)["q"] != "flaky" {
		t.Fatalf("%#v", out[0])
	}
}
