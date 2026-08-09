package pickpoint_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/pickpoint"
)

const e2eBatchSize = 1000

type timingTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	ms   []float64
}

func (t *timingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	start := time.Now()
	resp, err := base.RoundTrip(req)
	elapsed := time.Since(start).Seconds() * 1000
	t.mu.Lock()
	t.ms = append(t.ms, elapsed)
	t.mu.Unlock()
	return resp, err
}

func (t *timingTransport) samples() []float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]float64, len(t.ms))
	copy(out, t.ms)
	return out
}

func e2eClient(t *testing.T, tt *timingTransport) *pickpoint.Client {
	t.Helper()
	key := os.Getenv("PICKPOINT_API_KEY")
	if key == "" {
		t.Skip("PICKPOINT_API_KEY not set")
	}
	cfg := pickpoint.Config{
		APIKey:  key,
		BaseURL: "https://beta-api.pickpoint.io",
		HTTPClient: &http.Client{
			Timeout:   60 * time.Second,
			Transport: tt,
		},
	}
	if base := os.Getenv("PICKPOINT_BASE_URL"); base != "" {
		cfg.BaseURL = base
	}
	c, err := pickpoint.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	// Nearest-rank
	rank := int((p / 100) * float64(len(sorted)))
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	if rank < 0 {
		rank = 0
	}
	return sorted[rank]
}

func reportLatency(t *testing.T, label string, wall time.Duration, samples []float64) {
	t.Helper()
	if len(samples) == 0 {
		t.Fatalf("%s: no latency samples", label)
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(len(sorted))
	msg := fmt.Sprintf(
		"%s batch n=%d wall=%s | per-request ms: min=%.1f p50=%.1f p90=%.1f p95=%.1f p99=%.1f max=%.1f mean=%.1f",
		label,
		len(sorted),
		wall.Round(time.Millisecond),
		sorted[0],
		percentile(sorted, 50),
		percentile(sorted, 90),
		percentile(sorted, 95),
		percentile(sorted, 99),
		sorted[len(sorted)-1],
		mean,
	)
	t.Log(msg)
	fmt.Println(msg)
}

func TestE2EForwardBatch100(t *testing.T) {
	tt := &timingTransport{}
	c := e2eClient(t, tt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	qs := make([]pickpoint.Query, e2eBatchSize)
	for i := range qs {
		qs[i] = pickpoint.Query{"q": "Berlin", "limit": "1"}
	}

	start := time.Now()
	out, err := c.ForwardBatch(ctx, qs)
	wall := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != e2eBatchSize {
		t.Fatalf("len=%d want %d", len(out), e2eBatchSize)
	}
	for i, slot := range out {
		if len(slot) == 0 {
			t.Fatalf("slot %d empty", i)
		}
	}
	reportLatency(t, "forward", wall, tt.samples())
}

func TestE2EReverseBatch100(t *testing.T) {
	tt := &timingTransport{}
	c := e2eClient(t, tt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	qs := make([]pickpoint.Query, e2eBatchSize)
	for i := range qs {
		// Brandenburg Gate
		qs[i] = pickpoint.Query{"lat": "52.5163", "lon": "13.3777"}
	}

	start := time.Now()
	out, err := c.ReverseBatch(ctx, qs)
	wall := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != e2eBatchSize {
		t.Fatalf("len=%d want %d", len(out), e2eBatchSize)
	}
	for i, slot := range out {
		if slot == nil {
			t.Fatalf("slot %d nil", i)
		}
	}
	reportLatency(t, "reverse", wall, tt.samples())
}
