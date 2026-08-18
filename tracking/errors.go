package tracking

import "fmt"

// Error is a typed tracking protocol / SDK error.
type Error struct {
	Code         ErrorCode
	Message      string
	RetryAfterMs uint32
	TrackUID     string
}

func (e *Error) Error() string {
	if e == nil {
		return "tracking: error"
	}
	if e.Message != "" {
		return fmt.Sprintf("tracking: %s (%s)", e.Message, e.Code)
	}
	return fmt.Sprintf("tracking: %s", e.Code)
}

// NewError builds an Error from a wire code + message.
func NewError(code ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

func errorFromWire(err *WireError) *Error {
	if err == nil {
		return NewError(ErrorInvalid, "unknown error")
	}
	return &Error{
		Code:         err.Code,
		Message:      err.Message,
		RetryAfterMs: err.RetryAfterMs,
		TrackUID:     err.TrackUID,
	}
}

// Fatal resume: AUTH, TRACK_NOT_FOUND only. FENCED / TRY_AGAIN retry Resume.
func isFatalResumeError(code ErrorCode) bool {
	switch code {
	case ErrorTrackNotFound, ErrorAuth:
		return true
	default:
		return false
	}
}

func isRetryResumeError(code ErrorCode) bool {
	return code == ErrorFenced || code == ErrorTryAgain
}

func isAuthError(code ErrorCode) bool {
	return code == ErrorAuth || code == ErrorUnauthorized
}
