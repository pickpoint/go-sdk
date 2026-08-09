package pickpoint

import (
	"errors"
	"fmt"
)

// Sentinel errors for callers using errors.Is.
var (
	ErrAuth         = errors.New("pickpoint: auth failed")
	ErrNotFound     = errors.New("pickpoint: not found")
	ErrConflict     = errors.New("pickpoint: conflict")
	ErrInvalidConfig = errors.New("pickpoint: invalid config")
)

// APIError is a non-2xx public-api response (or transport failure after retries).
type APIError struct {
	Status  int
	Code    string
	Message string
	Body    []byte
	Err     error
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("pickpoint: %s (status=%d code=%s)", e.Message, e.Status, e.Code)
	}
	return fmt.Sprintf("pickpoint: request failed (status=%d code=%s)", e.Status, e.Code)
}

func (e *APIError) Unwrap() error {
	switch e.Code {
	case "API_AUTH", "REFRESH_FAILED":
		return ErrAuth
	case "NOT_FOUND":
		return ErrNotFound
	case "CONFLICT":
		return ErrConflict
	case "INVALID_CONFIG":
		return ErrInvalidConfig
	default:
		return e.Err
	}
}
