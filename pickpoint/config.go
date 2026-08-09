package pickpoint

import (
	"net/http"
	"time"
)

const (
	DefaultBaseURL     = "https://api.pickpoint.io"
	DefaultMaxRetries  = 3
	DefaultRetryBase   = time.Second
	MinRetryBase       = 200 * time.Millisecond
	DefaultTimeout     = 30 * time.Second
	MaxConcurrency     = 20
	DefaultConcurrency = 20
	clientAuthRefreshAt = 0.5 // refresh when half of access TTL elapsed
)

// ClientAuth is a pair from POST /v2/client-tokens (via your backend).
// ExpiresAt is unix epoch milliseconds.
type ClientAuth struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
}

// Config configures the public-api client.
// Provide exactly one of APIKey, ClientAuth, or AccessToken.
type Config struct {
	// APIKey is the secret key (x-api-key). Prefer for server-side Go.
	APIKey string

	// ClientAuth is a short-lived SPA pair. SDK refreshes at ~50% TTL and on 401.
	ClientAuth *ClientAuth

	// AccessToken is a static Bearer (not refreshable). Prefer ClientAuth.
	AccessToken string

	// BaseURL defaults to DefaultBaseURL.
	BaseURL string

	// HTTPClient overrides the default client (with Timeout).
	HTTPClient *http.Client

	// MaxRetries after 5xx / network errors. Default 3.
	MaxRetries int

	// RetryBase is the exponential backoff base. Default 1s; min MinRetryBase.
	RetryBase time.Duration

	// Timeout per attempt. Default 30s. Ignored if HTTPClient is set with its own Timeout
	// unless HTTPClient is nil.
	Timeout time.Duration

	// Concurrency caps parallel geocode batch requests. Default and max: MaxConcurrency.
	Concurrency int
}
