package tracking

import (
	"fmt"

	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

// Error is a typed tracking protocol / SDK error.
type Error struct {
	Code    pb.ErrorCode
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return "tracking: error"
	}
	if e.Message != "" {
		return fmt.Sprintf("tracking: %s (%v)", e.Message, e.Code)
	}
	return fmt.Sprintf("tracking: %v", e.Code)
}

// NewError builds an Error from a wire code + message.
func NewError(code pb.ErrorCode, message string) *Error {
	return &Error{Code: code, Message: message}
}

func errorFromWire(err *pb.Error) *Error {
	if err == nil {
		return NewError(pb.ErrorCode_ERROR_CODE_INVALID, "unknown error")
	}
	return NewError(err.GetCode(), err.GetMessage())
}

func isFatalResumeError(code pb.ErrorCode) bool {
	switch code {
	case pb.ErrorCode_ERROR_CODE_TRACK_NOT_FOUND,
		pb.ErrorCode_ERROR_CODE_FENCED,
		pb.ErrorCode_ERROR_CODE_AUTH,
		pb.ErrorCode_ERROR_CODE_UNAUTHORIZED:
		return true
	default:
		return false
	}
}

func isAuthError(code pb.ErrorCode) bool {
	return code == pb.ErrorCode_ERROR_CODE_AUTH || code == pb.ErrorCode_ERROR_CODE_UNAUTHORIZED
}
