package pickpoint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strconv"
)

// Device is a public-api device row (extra fields preserved via raw JSON if needed).
type Device struct {
	ID          int64           `json:"id"`
	UID         string          `json:"uid"`
	Name        string          `json:"name"`
	Status      string          `json:"status"`
	Description *string         `json:"description"`
	TracksCount int64           `json:"tracksCount"`
	Type        string          `json:"type"`
	Secret      string          `json:"secret"`
	Metadata    *string         `json:"metadata"`
	CreatedAt   string          `json:"createdAt"`
	UpdatedAt   string          `json:"updatedAt"`
	LastLocation json.RawMessage `json:"lastLocation"`
}

// DeviceInput is create/update body.
type DeviceInput struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Description *string `json:"description,omitempty"`
	Metadata    *string `json:"metadata,omitempty"`
}

// DeviceListResult is GET /v2/devices.
type DeviceListResult struct {
	Data  []Device `json:"data"`
	Total int64    `json:"total"`
}

// DeviceListQuery optional list filters.
type DeviceListQuery struct {
	Skip   int
	Take   int
	Search string
	Idle   bool
}

// DeviceCommandResult is POST /v2/devices/{uid}/command.
type DeviceCommandResult struct {
	Delivered uint64 `json:"delivered"`
}

// DevicesService wraps /v2/devices*.
type DevicesService struct{ c *Client }

func (s *DevicesService) List(ctx context.Context, q DeviceListQuery) (*DeviceListResult, error) {
	vals := url.Values{}
	if q.Skip > 0 {
		vals.Set("skip", itoa(q.Skip))
	}
	if q.Take > 0 {
		vals.Set("take", itoa(q.Take))
	}
	if q.Search != "" {
		vals.Set("search", q.Search)
	}
	if q.Idle {
		vals.Set("idle", "1")
	}
	raw, err := s.c.do(ctx, requestOpts{path: "/v2/devices", query: vals})
	if err != nil {
		return nil, err
	}
	var out DeviceListResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *DevicesService) Get(ctx context.Context, uid string) (*Device, error) {
	raw, err := s.c.do(ctx, requestOpts{path: "/v2/devices/" + url.PathEscape(uid)})
	if err != nil {
		return nil, err
	}
	var d Device
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *DevicesService) Create(ctx context.Context, in DeviceInput) (*Device, error) {
	raw, err := s.c.do(ctx, requestOpts{
		method: "POST",
		path:   "/v2/devices",
		body:   in,
	})
	if err != nil {
		return nil, err
	}
	var d Device
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *DevicesService) Update(ctx context.Context, uid string, in DeviceInput) (*Device, error) {
	raw, err := s.c.do(ctx, requestOpts{
		method: "PATCH",
		path:   "/v2/devices/" + url.PathEscape(uid),
		body:   in,
	})
	if err != nil {
		return nil, err
	}
	var d Device
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *DevicesService) Delete(ctx context.Context, uid string) error {
	_, err := s.c.do(ctx, requestOpts{
		method: "DELETE",
		path:   "/v2/devices/" + url.PathEscape(uid),
	})
	return err
}

// Command injects opaque bytes into an online device session.
// payload may be raw bytes (base64-encoded by the SDK) or an already-encoded base64 string
// if you pass []byte of the ASCII base64 — prefer raw bytes.
func (s *DevicesService) Command(ctx context.Context, uid string, payload []byte) (*DeviceCommandResult, error) {
	raw, err := s.c.do(ctx, requestOpts{
		method: "POST",
		path:   "/v2/devices/" + url.PathEscape(uid) + "/command",
		body: map[string]string{
			"payload": base64.StdEncoding.EncodeToString(payload),
		},
	})
	if err != nil {
		return nil, err
	}
	var out DeviceCommandResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func itoa(n int) string { return strconv.Itoa(n) }
