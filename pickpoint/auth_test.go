package pickpoint_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestClientAuthRefreshOn401(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/client-tokens/refresh") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "access-2",
				"refreshToken": "refresh-2",
				"expiresAt":    time.Now().Add(time.Minute).UnixMilli(),
			})
			return
		}
		auth := r.Header.Get("Authorization")
		i := n.Add(1)
		if i == 1 {
			if auth != "Bearer access-1" {
				t.Errorf("first auth %q", auth)
			}
			w.WriteHeader(401)
			return
		}
		if auth != "Bearer access-2" {
			t.Errorf("second auth %q", auth)
		}
		_, _ = w.Write([]byte(`[{"ok":true}]`))
	}))
	defer srv.Close()

	c, err := pickpoint.New(pickpoint.Config{
		BaseURL: srv.URL,
		ClientAuth: &pickpoint.ClientAuth{
			AccessToken:  "access-1",
			RefreshToken: "refresh-1",
			ExpiresAt:    time.Now().Add(time.Minute).UnixMilli(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Forward(context.Background(), pickpoint.Query{"q": "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("%#v", out)
	}
}

func TestSingleFlightRefresh(t *testing.T) {
	var refreshes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refresh") {
			refreshes.Add(1)
			time.Sleep(40 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "access-fresh",
				"refreshToken": "refresh-2",
				"expiresAt":    time.Now().Add(2 * time.Minute).UnixMilli(),
			})
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer access-fresh" {
			t.Errorf("auth %q", got)
		}
		_, _ = w.Write([]byte(`[{"ok":true}]`))
	}))
	defer srv.Close()

	c, err := pickpoint.New(pickpoint.Config{
		BaseURL: srv.URL,
		ClientAuth: &pickpoint.ClientAuth{
			AccessToken:  "stale",
			RefreshToken: "refresh-1",
			ExpiresAt:    time.Now().Add(80 * time.Millisecond).UnixMilli(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Forward(ctx, pickpoint.Query{"q": "x"}); err != nil {
				t.Errorf("forward: %v", err)
			}
		}()
	}
	wg.Wait()
	if refreshes.Load() != 1 {
		t.Fatalf("refreshes=%d want 1", refreshes.Load())
	}
}

func TestRefreshRotationSecondClientFails(t *testing.T) {
	var mu sync.Mutex
	valid := "refresh-1"
	handler := func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refresh") {
			var body struct {
				RefreshToken string `json:"refreshToken"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			ok := body.RefreshToken == valid
			if ok {
				valid = "refresh-2"
			}
			mu.Unlock()
			if !ok {
				w.WriteHeader(401)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "a2",
				"refreshToken": "refresh-2",
				"expiresAt":    time.Now().Add(time.Minute).UnixMilli(),
			})
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}
	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	mk := func() *pickpoint.Client {
		c, err := pickpoint.New(pickpoint.Config{
			BaseURL: srv.URL,
			ClientAuth: &pickpoint.ClientAuth{
				AccessToken:  "a1",
				RefreshToken: "refresh-1",
				ExpiresAt:    time.Now().Add(50 * time.Millisecond).UnixMilli(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	a := mk()
	time.Sleep(40 * time.Millisecond)
	if _, err := a.Forward(context.Background(), pickpoint.Query{"q": "a"}); err != nil {
		t.Fatal(err)
	}

	b := mk()
	time.Sleep(40 * time.Millisecond)
	_, err := b.Forward(context.Background(), pickpoint.Query{"q": "b"})
	if err == nil || !errors.Is(err, pickpoint.ErrAuth) {
		t.Fatalf("want ErrAuth, got %v", err)
	}
}

func TestUnauthorizedRetryExactlyOnce(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refresh") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "a2",
				"refreshToken": "r2",
				"expiresAt":    time.Now().Add(time.Minute).UnixMilli(),
			})
			return
		}
		hits.Add(1)
		w.WriteHeader(401)
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		BaseURL: srv.URL,
		ClientAuth: &pickpoint.ClientAuth{
			AccessToken:  "a1",
			RefreshToken: "r1",
			ExpiresAt:    time.Now().Add(time.Minute).UnixMilli(),
		},
	})
	_, err := c.Forward(context.Background(), pickpoint.Query{"q": "x"})
	if !errors.Is(err, pickpoint.ErrAuth) {
		t.Fatalf("%v", err)
	}
	if hits.Load() != 2 {
		t.Fatalf("hits=%d want 2", hits.Load())
	}
}

func TestProactiveRefreshHalfwayTTL(t *testing.T) {
	var refreshed atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refresh") {
			refreshed.Store(true)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "a2",
				"refreshToken": "r2",
				"expiresAt":    time.Now().Add(time.Minute).UnixMilli(),
			})
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	ttl := 200 * time.Millisecond
	c, _ := pickpoint.New(pickpoint.Config{
		BaseURL: srv.URL,
		ClientAuth: &pickpoint.ClientAuth{
			AccessToken:  "a1",
			RefreshToken: "r1",
			ExpiresAt:    time.Now().Add(ttl).UnixMilli(),
		},
	})
	ctx := context.Background()
	if _, err := c.Forward(ctx, pickpoint.Query{"q": "early"}); err != nil {
		t.Fatal(err)
	}
	if refreshed.Load() {
		t.Fatal("refreshed too early")
	}
	time.Sleep(ttl*55/100 + 10*time.Millisecond)
	if _, err := c.Forward(ctx, pickpoint.Query{"q": "late"}); err != nil {
		t.Fatal(err)
	}
	if !refreshed.Load() {
		t.Fatal("expected proactive refresh")
	}
}

func TestMixedFanOutShares401Refresh(t *testing.T) {
	var refreshes atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refresh") {
			refreshes.Add(1)
			time.Sleep(20 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"accessToken":  "a2",
				"refreshToken": "r2",
				"expiresAt":    time.Now().Add(time.Minute).UnixMilli(),
			})
			return
		}
		if r.Header.Get("Authorization") == "Bearer a1" {
			w.WriteHeader(401)
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/address/search"):
			_, _ = w.Write([]byte(`{"features":[]}`))
		case strings.Contains(r.URL.Path, "/devices"):
			_, _ = w.Write([]byte(`{"data":[],"total":0}`))
		default:
			_, _ = w.Write([]byte(`[{"ok":true}]`))
		}
	}))
	defer srv.Close()

	c, _ := pickpoint.New(pickpoint.Config{
		BaseURL: srv.URL,
		ClientAuth: &pickpoint.ClientAuth{
			AccessToken:  "a1",
			RefreshToken: "r1",
			ExpiresAt:    time.Now().Add(time.Minute).UnixMilli(),
		},
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); _, _ = c.Forward(ctx, pickpoint.Query{"q": "a"}) }()
	go func() { defer wg.Done(); _, _ = c.Search(ctx, pickpoint.Query{"q": "b"}) }()
	go func() { defer wg.Done(); _, _ = c.Devices.List(ctx, pickpoint.DeviceListQuery{}) }()
	wg.Wait()
	if refreshes.Load() != 1 {
		t.Fatalf("refreshes=%d", refreshes.Load())
	}
}

func TestMintClientTokens(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "secret" {
			t.Fatal("need api key")
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"geocoding"`) {
			t.Fatalf("body %s", body)
		}
		_ = json.NewEncoder(w).Encode(pickpoint.TokenPair{
			AccessToken:  "a",
			RefreshToken: "r",
			ExpiresAt:    123,
			ExpiresIn:    600,
			Scopes:       []string{"geocoding"},
		})
	}))
	defer srv.Close()

	pair, err := pickpoint.MintClientTokens(context.Background(), pickpoint.Config{
		APIKey:  "secret",
		BaseURL: srv.URL,
	}, []string{"geocoding"}, 600)
	if err != nil {
		t.Fatal(err)
	}
	if pair.AccessToken != "a" {
		t.Fatalf("%#v", pair)
	}
}

func TestMintClientTokensScopes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "secret" {
			t.Fatal("missing key")
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		scopes, _ := body["scopes"].([]any)
		if len(scopes) != 0 {
			t.Fatalf("want empty scopes, got %v", scopes)
		}
		_ = json.NewEncoder(w).Encode(pickpoint.TokenPair{
			AccessToken: "a", RefreshToken: "r", ExpiresAt: 1, Scopes: []string{"geocoding"},
		})
	}))
	defer srv.Close()

	pair, err := pickpoint.MintClientTokens(context.Background(), pickpoint.Config{
		APIKey: "secret", BaseURL: srv.URL,
	}, nil, 0)
	if err != nil || pair.AccessToken != "a" {
		t.Fatalf("%v %#v", err, pair)
	}
}

func TestMintClientTokensWithScopes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(raw), `"devices"`) {
			t.Fatalf("%s", raw)
		}
		_ = json.NewEncoder(w).Encode(pickpoint.TokenPair{AccessToken: "a", RefreshToken: "r", ExpiresAt: 1})
	}))
	defer srv.Close()
	_, err := pickpoint.MintClientTokens(context.Background(), pickpoint.Config{
		APIKey: "k", BaseURL: srv.URL,
	}, []string{"geocoding", "devices"}, 600)
	if err != nil {
		t.Fatal(err)
	}
}
