package pickpoint

import (
	"context"
	"encoding/json"
)

// AddressService wraps Photon search (`GET /v2/address/search`).
type AddressService struct{ c *Client }

// Search runs address autocomplete / place search.
func (s *AddressService) Search(ctx context.Context, q Query) (map[string]any, error) {
	raw, err := s.c.do(ctx, requestOpts{
		path:  "/v2/address/search",
		query: queryFromMap(q),
	})
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Search is a flat shortcut for Address.Search.
func (c *Client) Search(ctx context.Context, q Query) (map[string]any, error) {
	return c.Address.Search(ctx, q)
}
