package tracking

import (
	"context"
	"fmt"
)

// Transport selects the edge protocol.
type Transport int

const (
	// TransportGRPC is the default for Go agents and the device-simulator.
	TransportGRPC Transport = iota
	// TransportWS talks binary protobuf on /v2/tracking/ws.
	TransportWS
)

// DeviceAuth authenticates a publisher device.
type DeviceAuth struct {
	ClientID     string
	ClientSecret string
}

// ListenerAuth authenticates a dashboard/listener JWT.
type ListenerAuth struct {
	AccessToken string
}

// Config opens a session against a Pickpoint tracking endpoint.
type Config struct {
	// Endpoint host, e.g. "tracking.example.com:443" (gRPC) or "wss://…".
	Endpoint  string
	Transport Transport
	Device    *DeviceAuth
	Listener  *ListenerAuth
}

// Client is a tracking session (device or listener).
type Client struct {
	cfg Config
}

// Connect opens a tracking session.
// Not implemented until the Rust edge and generated stubs are wired.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	_ = ctx
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("tracking: Endpoint is required")
	}
	if cfg.Device == nil && cfg.Listener == nil {
		return nil, fmt.Errorf("tracking: Device or Listener auth is required")
	}
	return nil, fmt.Errorf("tracking: Connect not implemented yet")
}

// Close ends the session.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	return nil
}
