package pickpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type onClientError int

const (
	onClientErrorThrow onClientError = iota
	onClientErrorEmpty
)

type requestOpts struct {
	method        string
	path          string
	query         url.Values
	body          any
	onClientError onClientError
	empty         func() ([]byte, error)
}

func (c *Client) do(ctx context.Context, opts requestOpts) ([]byte, error) {
	attempt := 0
	authRetried := false

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		u := c.baseURL + opts.path
		if len(opts.query) > 0 {
			u += "?" + opts.query.Encode()
		}

		var bodyReader io.Reader
		if opts.body != nil {
			raw, err := json.Marshal(opts.body)
			if err != nil {
				return nil, err
			}
			bodyReader = bytes.NewReader(raw)
		}

		method := opts.method
		if method == "" {
			if opts.body != nil {
				method = http.MethodPost
			} else {
				method = http.MethodGet
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, u, bodyReader)
		if err != nil {
			return nil, err
		}
		if opts.body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if err := c.auth.apply(ctx, req); err != nil {
			return nil, err
		}

		res, err := c.http.Do(req)
		if err != nil {
			if attempt >= c.maxRetries {
				return nil, &APIError{Code: "NETWORK", Message: "network error", Err: err}
			}
			sleepBackoff(ctx, c.retryBase, attempt)
			attempt++
			continue
		}

		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		switch {
		case res.StatusCode == http.StatusUnauthorized:
			if !authRetried && c.auth.kind == authBearer && c.auth.session.refreshAfterUnauthorized(ctx) {
				authRetried = true
				continue
			}
			return nil, &APIError{Status: res.StatusCode, Code: "API_AUTH", Message: "auth failed (401)", Body: raw}

		case res.StatusCode == http.StatusPaymentRequired || res.StatusCode == http.StatusForbidden:
			return nil, &APIError{Status: res.StatusCode, Code: "API_AUTH", Message: "auth failed", Body: raw}

		case res.StatusCode == http.StatusNoContent:
			return nil, nil

		case res.StatusCode == http.StatusConflict:
			return nil, &APIError{
				Status:  409,
				Code:    "CONFLICT",
				Message: messageFromBody(raw, 409),
				Body:    raw,
			}

		case res.StatusCode == http.StatusBadRequest || (res.StatusCode >= 404 && res.StatusCode < 500):
			if opts.onClientError == onClientErrorEmpty {
				if opts.empty != nil {
					return opts.empty()
				}
				return nil, nil
			}
			code := "CLIENT_ERROR"
			if res.StatusCode == http.StatusNotFound {
				code = "NOT_FOUND"
			}
			return nil, &APIError{
				Status:  res.StatusCode,
				Code:    code,
				Message: messageFromBody(raw, res.StatusCode),
				Body:    raw,
			}

		case res.StatusCode >= 500:
			if attempt >= c.maxRetries {
				return nil, &APIError{
					Status:  res.StatusCode,
					Code:    "SERVER_ERROR",
					Message: "server error after retries",
					Body:    raw,
				}
			}
			sleepBackoff(ctx, c.retryBase, attempt)
			attempt++
			continue

		case res.StatusCode >= 200 && res.StatusCode < 300:
			return raw, nil

		default:
			if res.StatusCode >= 400 && res.StatusCode < 500 && opts.onClientError == onClientErrorEmpty {
				if opts.empty != nil {
					return opts.empty()
				}
				return nil, nil
			}
			return nil, &APIError{
				Status:  res.StatusCode,
				Code:    "CLIENT_ERROR",
				Message: messageFromBody(raw, res.StatusCode),
				Body:    raw,
			}
		}
	}
}

func messageFromBody(raw []byte, status int) string {
	var m struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if json.Unmarshal(raw, &m) == nil {
		if m.Message != "" {
			return m.Message
		}
		if m.Error != "" {
			return m.Error
		}
	}
	return http.StatusText(status)
}

func sleepBackoff(ctx context.Context, base time.Duration, attempt int) {
	if base <= 0 {
		base = DefaultRetryBase
	}
	// full jitter: [0, base*2^attempt)
	max := base << attempt
	d := time.Duration(rand.Int63n(int64(max) + 1))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func queryFromMap(m map[string]string) url.Values {
	q := url.Values{}
	for k, v := range m {
		if v == "" {
			continue
		}
		q.Set(k, v)
	}
	return q
}

func trimSlash(s string) string {
	return strings.TrimRight(s, "/")
}
