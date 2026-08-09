package pickpoint

import (
	"net/http"
	"time"
)

// Client is the unified public-api client (geocoding, address, routing, devices).
// Tracking (WebSocket) lives in package tracking — different lifecycle.
type Client struct {
	baseURL   string
	http      *http.Client
	auth      *authState
	maxRetries int
	retryBase time.Duration
	concurrency int

	Geocoding *GeocodingService
	Address   *AddressService
	Routing   *RoutingService
	Devices   *DevicesService
}

// New builds a Client. Provide exactly one of Config.APIKey, ClientAuth, AccessToken.
func New(cfg Config) (*Client, error) {
	base := cfg.BaseURL
	if base == "" {
		base = DefaultBaseURL
	}
	base = trimSlash(base)

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: timeout}
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}
	retryBase := cfg.RetryBase
	if retryBase <= 0 {
		retryBase = DefaultRetryBase
	}
	if retryBase < MinRetryBase {
		retryBase = MinRetryBase
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	if concurrency > MaxConcurrency {
		concurrency = MaxConcurrency
	}

	auth, err := resolveAuth(cfg, base, hc)
	if err != nil {
		return nil, err
	}

	c := &Client{
		baseURL:     base,
		http:        hc,
		auth:        auth,
		maxRetries:  maxRetries,
		retryBase:   retryBase,
		concurrency: concurrency,
	}
	c.Geocoding = &GeocodingService{c: c}
	c.Address = &AddressService{c: c}
	c.Routing = &RoutingService{c: c}
	c.Devices = &DevicesService{c: c}
	return c, nil
}
