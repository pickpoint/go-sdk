package pickpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// TokenPair is the response from POST /v2/client-tokens.
type TokenPair struct {
	AccessToken  string   `json:"accessToken"`
	RefreshToken string   `json:"refreshToken"`
	ExpiresAt    int64    `json:"expiresAt"`
	ExpiresIn    int64    `json:"expiresIn"`
	Scopes       []string `json:"scopes"`
}

// MintClientTokens mints a client-token pair with a secret API key (server-side).
// Pass nil/empty scopes to grant all client-tokenable permissions on the key.
func MintClientTokens(ctx context.Context, cfg Config, scopes []string, ttlSec int64) (*TokenPair, error) {
	if cfg.APIKey == "" {
		return nil, &APIError{Code: "INVALID_CONFIG", Message: "MintClientTokens requires APIKey"}
	}
	base := cfg.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	base = trimSlash(base)
	hc := cfg.HTTPClient
	if hc == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = DefaultTimeout
		}
		hc = &http.Client{Timeout: timeout}
	}
	payload := map[string]any{"scopes": scopes}
	if scopes == nil {
		payload["scopes"] = []string{}
	}
	if ttlSec > 0 {
		payload["ttlSec"] = ttlSec
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v2/client-tokens", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", cfg.APIKey)
	res, err := hc.Do(req)
	if err != nil {
		return nil, &APIError{Code: "NETWORK", Message: "mint client tokens network error", Err: err}
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, &APIError{
			Status:  res.StatusCode,
			Code:    "CLIENT_ERROR",
			Message: fmt.Sprintf("mint client tokens failed (%d)", res.StatusCode),
			Body:    raw,
		}
	}
	var pair TokenPair
	if err := json.Unmarshal(raw, &pair); err != nil {
		return nil, err
	}
	return &pair, nil
}
