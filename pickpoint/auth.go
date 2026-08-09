package pickpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type authKind int

const (
	authAPIKey authKind = iota
	authBearer
)

type tokenSession interface {
	token(ctx context.Context) (string, error)
	refreshAfterUnauthorized(ctx context.Context) bool
}

type authState struct {
	kind    authKind
	apiKey  string
	session tokenSession
}

func resolveAuth(cfg Config, baseURL string, hc *http.Client) (*authState, error) {
	n := 0
	if cfg.APIKey != "" {
		n++
	}
	if cfg.ClientAuth != nil {
		n++
	}
	if cfg.AccessToken != "" {
		n++
	}
	if n > 1 {
		return nil, &APIError{
			Code:    "INVALID_CONFIG",
			Message: "provide only one of: APIKey | ClientAuth | AccessToken",
		}
	}
	if n == 0 {
		return nil, &APIError{
			Code:    "INVALID_CONFIG",
			Message: "auth required: APIKey, ClientAuth, or AccessToken",
		}
	}
	if cfg.APIKey != "" {
		return &authState{kind: authAPIKey, apiKey: cfg.APIKey}, nil
	}
	if cfg.ClientAuth != nil {
		s, err := newClientAuthSession(*cfg.ClientAuth, baseURL, hc)
		if err != nil {
			return nil, err
		}
		return &authState{kind: authBearer, session: s}, nil
	}
	return &authState{kind: authBearer, session: staticSession(cfg.AccessToken)}, nil
}

func (a *authState) apply(ctx context.Context, req *http.Request) error {
	req.Header.Set("Accept", "application/json")
	if a.kind == authAPIKey {
		req.Header.Set("x-api-key", a.apiKey)
		return nil
	}
	tok, err := a.session.token(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

type clientAuthSession struct {
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	expiresAt    int64
	issuedAt     time.Time
	baseURL      string
	hc           *http.Client
	refreshing   bool
	waiters      []chan error
}

func newClientAuthSession(initial ClientAuth, baseURL string, hc *http.Client) (*clientAuthSession, error) {
	if initial.AccessToken == "" || initial.RefreshToken == "" || initial.ExpiresAt == 0 {
		return nil, &APIError{
			Code:    "INVALID_CONFIG",
			Message: "ClientAuth requires AccessToken, RefreshToken, and ExpiresAt (unix ms)",
		}
	}
	return &clientAuthSession{
		accessToken:  initial.AccessToken,
		refreshToken: initial.RefreshToken,
		expiresAt:    initial.ExpiresAt,
		issuedAt:     time.Now(),
		baseURL:      baseURL,
		hc:           hc,
	}, nil
}

func (s *clientAuthSession) token(ctx context.Context) (string, error) {
	s.mu.Lock()
	need := s.needsProactiveRefreshLocked()
	s.mu.Unlock()
	if need {
		if err := s.refresh(ctx); err != nil {
			return "", err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accessToken, nil
}

func (s *clientAuthSession) refreshAfterUnauthorized(ctx context.Context) bool {
	return s.refresh(ctx) == nil
}

func (s *clientAuthSession) needsProactiveRefreshLocked() bool {
	ttlMs := s.expiresAt - s.issuedAt.UnixMilli()
	if ttlMs <= 0 {
		return time.Now().UnixMilli() >= s.expiresAt-30_000
	}
	refreshAt := s.issuedAt.Add(time.Duration(float64(ttlMs)*clientAuthRefreshAt) * time.Millisecond)
	return !time.Now().Before(refreshAt)
}

func (s *clientAuthSession) refresh(ctx context.Context) error {
	s.mu.Lock()
	if s.refreshing {
		ch := make(chan error, 1)
		s.waiters = append(s.waiters, ch)
		s.mu.Unlock()
		select {
		case err := <-ch:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.refreshing = true
	refreshTok := s.refreshToken
	s.mu.Unlock()

	err := s.doRefresh(ctx, refreshTok)

	s.mu.Lock()
	s.refreshing = false
	for _, w := range s.waiters {
		w <- err
	}
	s.waiters = nil
	s.mu.Unlock()
	return err
}

func (s *clientAuthSession) doRefresh(ctx context.Context, refreshTok string) error {
	body, _ := json.Marshal(map[string]string{"refreshToken": refreshTok})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL+"/v2/client-tokens/refresh", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	res, err := s.hc.Do(req)
	if err != nil {
		return &APIError{Code: "REFRESH_FAILED", Message: "client token refresh network error", Err: err}
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return &APIError{
			Status:  res.StatusCode,
			Code:    "REFRESH_FAILED",
			Message: fmt.Sprintf("client token refresh failed (%d)", res.StatusCode),
			Body:    raw,
		}
	}
	var pair ClientAuth
	if err := json.Unmarshal(raw, &pair); err != nil {
		return &APIError{Code: "INVALID_TOKEN", Message: "refresh returned invalid JSON", Err: err}
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" || pair.ExpiresAt == 0 {
		return &APIError{Code: "INVALID_TOKEN", Message: "refresh returned invalid clientAuth pair"}
	}
	s.mu.Lock()
	s.accessToken = pair.AccessToken
	s.refreshToken = pair.RefreshToken
	s.expiresAt = pair.ExpiresAt
	s.issuedAt = time.Now()
	s.mu.Unlock()
	return nil
}

type staticTok string

func staticSession(tok string) tokenSession { return staticTok(tok) }

func (t staticTok) token(context.Context) (string, error)         { return string(t), nil }
func (t staticTok) refreshAfterUnauthorized(context.Context) bool { return false }
