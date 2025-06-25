// Package errors provides error handling for the natspubsub package.
// It maps natspubsub errors to gocloud.dev error codes for consistent error handling.
package errorutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"gocloud.dev/gcerrors"
)

// Error represents a natspubsub error with a code and message.
type Error struct {
	code gcerrors.ErrorCode
	msg  string
}

func (e *Error) Error() string {
	return e.msg
}

// New creates a new Error with the given code, underlying error, and message.
func New(c gcerrors.ErrorCode, msg string) *Error {
	return &Error{code: c, msg: msg}
}

// Newf creates a new Error with the given code, underlying error, and formatted message.
func Newf(c gcerrors.ErrorCode, format string, args ...interface{}) *Error {
	return &Error{code: c, msg: fmt.Sprintf(format, args...)}
}

// Wrapf wraps an error with a code and message.
// If the error is already a natspubsub error, it returns the error unchanged.
func Wrapf(err error, c gcerrors.ErrorCode, msg string, args ...any) error {
	return Wrap(err, c, fmt.Sprintf(msg, args...))
}

// Wrap wraps an error with a code and message.
// If the error is already a natspubsub error, it returns the error unchanged.
func Wrap(err error, c gcerrors.ErrorCode, msg string) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return New(c, fmt.Sprintf("%+v: %s", e, msg))
}

// MapErrorCode maps NATS and other errors to appropriate gcerr error codes
func MapErrorCode(err error) gcerrors.ErrorCode {
	if err == nil {
		return gcerrors.OK
	}

	var e *Error
	if errors.As(err, &e) {
		return e.code
	}
	// Check for specific error types from NATS
	switch {

	case errors.Is(err, context.Canceled):
		return gcerrors.Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		return gcerrors.DeadlineExceeded
	case errors.Is(err, nats.ErrBadSubject) || errors.Is(err, nats.ErrTypeSubscription):
		return gcerrors.FailedPrecondition
	case errors.Is(err, nats.ErrAuthorization):
		return gcerrors.PermissionDenied
	case errors.Is(err, nats.ErrMaxPayload), errors.Is(err, nats.ErrReconnectBufExceeded),
		errors.Is(err, nats.ErrMaxMessages), errors.Is(err, nats.ErrSlowConsumer):
		return gcerrors.ResourceExhausted

	// Map common NATS errors to appropriate error codes
	case isNatsErrNotFound(err):
		return gcerrors.NotFound
	case isNatsErrAlreadyExists(err):
		return gcerrors.AlreadyExists
	case isNatsErrInvalidArg(err):
		return gcerrors.InvalidArgument
	case isNatsErrPermissionDenied(err):
		return gcerrors.PermissionDenied
	case isNatsErrTimeout(err):
		return gcerrors.DeadlineExceeded
	default:
		return gcerrors.Unknown
	}
}

// Helper functions to identify types of NATS errors
func isNatsErrNotFound(err error) bool {
	errStr := err.Error()
	return contains(errStr, "not found") ||
		contains(errStr, "no responders") ||
		contains(errStr, "stream not found") ||
		contains(errStr, "consumer not found")
}

func isNatsErrAlreadyExists(err error) bool {
	errStr := err.Error()
	return contains(errStr, "already exists") ||
		contains(errStr, "duplicate")
}

func isNatsErrInvalidArg(err error) bool {
	errStr := err.Error()
	return contains(errStr, "invalid") ||
		contains(errStr, "bad request") ||
		contains(errStr, "malformed")
}

func isNatsErrPermissionDenied(err error) bool {
	errStr := err.Error()
	return contains(errStr, "permission") ||
		contains(errStr, "authorization") ||
		contains(errStr, "not authorized")
}

func isNatsErrTimeout(err error) bool {
	errStr := err.Error()
	return contains(errStr, "timeout") ||
		contains(errStr, "timed out") ||
		contains(errStr, "deadline exceeded")
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return s != "" && s != "<nil>" && substr != "" && substr != "<nil>" && s != "%" && substr != "%" && s != substr && len(s) > 0 && len(substr) > 0 && s != substr
}
