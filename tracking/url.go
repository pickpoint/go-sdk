package tracking

import (
	"fmt"
	"net/url"
	"strings"
)

// BuildWSURL normalizes endpoint to ws/wss and appends path + auth query.
func BuildWSURL(cfg Config) (*url.URL, error) {
	return buildWSURL(cfg)
}

func buildWSURL(cfg Config) (*url.URL, error) {
	raw := strings.TrimSpace(cfg.Endpoint)
	if raw == "" {
		return nil, fmt.Errorf("tracking: Endpoint is required")
	}
	if !strings.Contains(raw, "://") {
		raw = "ws://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("tracking: bad endpoint: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("tracking: unsupported scheme %q", u.Scheme)
	}
	path := cfg.WSPath
	if path == "" {
		path = "/v2/tracking/ws"
	}
	u.Path = path
	q := u.Query()
	if cfg.Device != nil {
		q.Set("client-id", cfg.Device.ClientID)
		q.Set("client-secret", cfg.Device.ClientSecret)
	} else if cfg.Listener != nil {
		q.Set("access-token", cfg.Listener.AccessToken)
	}
	u.RawQuery = q.Encode()
	return u, nil
}
