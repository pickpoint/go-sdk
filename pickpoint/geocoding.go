package pickpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Query is a loose query-string map for geocode / address endpoints.
type Query map[string]string

// GeocodingService wraps /v2/geocode/* and /v2/address/lookup.
type GeocodingService struct{ c *Client }

// Forward geocodes a place. On non-auth 4xx returns an empty slice (batch-friendly).
func (s *GeocodingService) Forward(ctx context.Context, q Query) ([]any, error) {
	raw, err := s.c.do(ctx, requestOpts{
		path:          "/v2/geocode/forward",
		query:         queryFromMap(q),
		onClientError: onClientErrorEmpty,
		empty:         func() ([]byte, error) { return []byte("[]"), nil },
	})
	if err != nil {
		return nil, err
	}
	return decodeJSONArray(raw)
}

// Reverse reverse-geocodes. On non-auth 4xx returns (nil, nil).
func (s *GeocodingService) Reverse(ctx context.Context, q Query) (map[string]any, error) {
	raw, err := s.c.do(ctx, requestOpts{
		path:          "/v2/geocode/reverse",
		query:         queryFromMap(q),
		onClientError: onClientErrorEmpty,
		empty:         func() ([]byte, error) { return []byte("null"), nil },
	})
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Lookup resolves OSM ids (`GET /v2/address/lookup`).
func (s *GeocodingService) Lookup(ctx context.Context, q Query) ([]any, error) {
	raw, err := s.c.do(ctx, requestOpts{
		path:          "/v2/address/lookup",
		query:         queryFromMap(q),
		onClientError: onClientErrorEmpty,
		empty:         func() ([]byte, error) { return []byte("[]"), nil },
	})
	if err != nil {
		return nil, err
	}
	return decodeJSONArray(raw)
}

// ForwardBatch fans out Forward with at most Client concurrency in flight.
// First fatal error (auth / 5xx after retries) aborts remaining work.
func (s *GeocodingService) ForwardBatch(ctx context.Context, qs []Query) ([][]any, error) {
	return runBatch(ctx, s.c.concurrency, qs, s.Forward)
}

// ReverseBatch fans out Reverse.
func (s *GeocodingService) ReverseBatch(ctx context.Context, qs []Query) ([]map[string]any, error) {
	return runBatch(ctx, s.c.concurrency, qs, s.Reverse)
}

// LookupBatch fans out Lookup.
func (s *GeocodingService) LookupBatch(ctx context.Context, qs []Query) ([][]any, error) {
	return runBatch(ctx, s.c.concurrency, qs, s.Lookup)
}

// Flat shortcuts on Client (same as JS PickPoint).

func (c *Client) Forward(ctx context.Context, q Query) ([]any, error) {
	return c.Geocoding.Forward(ctx, q)
}

func (c *Client) Reverse(ctx context.Context, q Query) (map[string]any, error) {
	return c.Geocoding.Reverse(ctx, q)
}

func (c *Client) Lookup(ctx context.Context, q Query) ([]any, error) {
	return c.Geocoding.Lookup(ctx, q)
}

func (c *Client) ForwardBatch(ctx context.Context, qs []Query) ([][]any, error) {
	return c.Geocoding.ForwardBatch(ctx, qs)
}

func (c *Client) ReverseBatch(ctx context.Context, qs []Query) ([]map[string]any, error) {
	return c.Geocoding.ReverseBatch(ctx, qs)
}

func decodeJSONArray(raw []byte) ([]any, error) {
	if len(raw) == 0 {
		return []any{}, nil
	}
	var out []any
	if err := json.Unmarshal(raw, &out); err == nil {
		return out, nil
	}
	var one any
	if err := json.Unmarshal(raw, &one); err != nil {
		return nil, err
	}
	if one == nil {
		return []any{}, nil
	}
	return []any{one}, nil
}

// runBatch is a fixed-size worker pool (conveyor): it keeps up to `concurrency`
// requests in flight at all times. When one finishes, the next input starts
// immediately — it does not wait to finish a full wave of N before starting more.
func runBatch[T any](
	ctx context.Context,
	concurrency int,
	inputs []Query,
	fn func(context.Context, Query) (T, error),
) ([]T, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type slot struct {
		i   int
		v   T
		err error
	}
	jobs := make(chan int)
	out := make(chan slot, len(inputs))
	var wg sync.WaitGroup

	workers := concurrency
	if workers > len(inputs) {
		workers = len(inputs)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				v, err := fn(ctx, inputs[i])
				out <- slot{i: i, v: v, err: err}
				if err != nil {
					cancel()
				}
			}
		}()
	}

	go func() {
	loop:
		for i := range inputs {
			select {
			case <-ctx.Done():
				break loop
			case jobs <- i:
			}
		}
		close(jobs)
		wg.Wait()
		close(out)
	}()

	results := make([]T, len(inputs))
	var firstErr error
	for s := range out {
		if s.err != nil && firstErr == nil {
			firstErr = s.err
		}
		if s.err == nil {
			results[s.i] = s.v
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("pickpoint: batch cancelled: %w", ctx.Err())
	}
	return results, nil
}
