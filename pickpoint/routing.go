package pickpoint

import (
	"context"
	"encoding/json"
)

// RoutingService wraps Valhalla proxies under /v2/route*.
type RoutingService struct{ c *Client }

func (s *RoutingService) post(ctx context.Context, path string, body any) (json.RawMessage, error) {
	raw, err := s.c.do(ctx, requestOpts{
		method: "POST",
		path:   path,
		body:   body,
	})
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func (s *RoutingService) Route(ctx context.Context, body any) (json.RawMessage, error) {
	return s.post(ctx, "/v2/route", body)
}

func (s *RoutingService) Optimized(ctx context.Context, body any) (json.RawMessage, error) {
	return s.post(ctx, "/v2/route/optimized", body)
}

func (s *RoutingService) Matrix(ctx context.Context, body any) (json.RawMessage, error) {
	return s.post(ctx, "/v2/route/matrix", body)
}

func (s *RoutingService) Locate(ctx context.Context, body any) (json.RawMessage, error) {
	return s.post(ctx, "/v2/route/locate", body)
}

func (s *RoutingService) Elevation(ctx context.Context, body any) (json.RawMessage, error) {
	return s.post(ctx, "/v2/route/elevation", body)
}

func (c *Client) Route(ctx context.Context, body any) (json.RawMessage, error) {
	return c.Routing.Route(ctx, body)
}

func (c *Client) OptimizedRoute(ctx context.Context, body any) (json.RawMessage, error) {
	return c.Routing.Optimized(ctx, body)
}

func (c *Client) Matrix(ctx context.Context, body any) (json.RawMessage, error) {
	return c.Routing.Matrix(ctx, body)
}

func (c *Client) Locate(ctx context.Context, body any) (json.RawMessage, error) {
	return c.Routing.Locate(ctx, body)
}

func (c *Client) Elevation(ctx context.Context, body any) (json.RawMessage, error) {
	return c.Routing.Elevation(ctx, body)
}
