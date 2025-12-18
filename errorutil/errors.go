// Package errors provides error handling for the natspubsub package.
// It maps natspubsub errors to gocloud.dev error codes for consistent error handling.
package errorutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
func Newf(c gcerrors.ErrorCode, format string, args ...any) *Error {
	return &Error{code: c, msg: fmt.Sprintf(format, args...)}
}

// Wrapf wraps an error with a code and message.
// If the error is already a natspubsub error, it returns the error unchanged.
func Wrapf(err error, msg string, args ...any) error {
	return Wrap(err, fmt.Sprintf(msg, args...))
}

// Wrap wraps an error with a code and message.
// If the error is already a natspubsub error, it returns the error unchanged.
func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}

	errCode := MapErrorCode(err)
	return New(errCode, fmt.Sprintf("%s : %+v", msg, err))
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
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout),
		errors.Is(err, nats.ErrDrainTimeout):
		return gcerrors.DeadlineExceeded
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionDraining),
		errors.Is(err, nats.ErrDisconnected), errors.Is(err, nats.ErrInvalidConnection),
		errors.Is(err, nats.ErrStaleConnection):
		return gcerrors.Internal
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired),
		errors.Is(err, nats.ErrPermissionViolation):
		return gcerrors.PermissionDenied
	case errors.Is(err, nats.ErrBadSubject), errors.Is(err, nats.ErrTypeSubscription),
		errors.Is(err, nats.ErrBadQueueName),
		errors.Is(err, nats.ErrSyncSubRequired), errors.Is(err, nats.ErrInvalidMsg),
		errors.Is(err, nats.ErrMsgNotBound), errors.Is(err, nats.ErrMsgNoReply):
		return gcerrors.FailedPrecondition
	case errors.Is(err, nats.ErrMaxPayload), errors.Is(err, nats.ErrReconnectBufExceeded),
		errors.Is(err, nats.ErrMaxMessages), errors.Is(err, nats.ErrSlowConsumer),
		errors.Is(err, nats.ErrMaxConnectionsExceeded), errors.Is(err, nats.ErrMaxAccountConnectionsExceeded),
		errors.Is(err, nats.ErrMaxSubscriptionsExceeded):
		return gcerrors.ResourceExhausted
	case errors.Is(err, nats.ErrInvalidArg), errors.Is(err, nats.ErrBadTimeout),
		errors.Is(err, nats.ErrNoServers), errors.Is(err, nats.ErrJsonParse),
		errors.Is(err, nats.ErrChanArg), errors.Is(err, nats.ErrMultipleTLSConfigs),
		errors.Is(err, nats.ErrClientCertOrRootCAsRequired), errors.Is(err, nats.ErrInvalidContext),
		errors.Is(err, nats.ErrNoDeadlineContext), errors.Is(err, nats.ErrNoEchoNotSupported),
		errors.Is(err, nats.ErrClientIDNotSupported), errors.Is(err, nats.ErrClientIPNotSupported),
		errors.Is(err, nats.ErrHeadersNotSupported), errors.Is(err, nats.ErrBadHeaderMsg),
		errors.Is(err, nats.ErrConnectionNotTLS), errors.Is(err, nats.ErrNkeysNotSupported),
		errors.Is(err, nats.ErrUserButNoSigCB), errors.Is(err, nats.ErrNkeyButNoSigCB),
		errors.Is(err, nats.ErrNoUserCB), errors.Is(err, nats.ErrNkeyAndUser),
		errors.Is(err, nats.ErrTokenAlreadySet), errors.Is(err, nats.ErrUserInfoAlreadySet):
		return gcerrors.InvalidArgument
	case errors.Is(err, nats.ErrNoResponders), errors.Is(err, nats.ErrBadSubscription):
		return gcerrors.NotFound

	// Check for JetStream specific errors using errors.Is()
	case errors.Is(err, jetstream.ErrJetStreamNotEnabled), errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return gcerrors.Internal
	case errors.Is(err, jetstream.ErrStreamNotFound), errors.Is(err, jetstream.ErrConsumerNotFound),
		errors.Is(err, jetstream.ErrMsgNotFound), errors.Is(err, jetstream.ErrConsumerDoesNotExist),
		errors.Is(err, jetstream.ErrNoMessages), errors.Is(err, jetstream.ErrNoStreamResponse),
		errors.Is(err, jetstream.ErrKeyNotFound), errors.Is(err, jetstream.ErrBucketNotFound),
		errors.Is(err, jetstream.ErrNoKeysFound):
		return gcerrors.NotFound
	case errors.Is(err, jetstream.ErrStreamNameAlreadyInUse), errors.Is(err, jetstream.ErrConsumerExists),
		errors.Is(err, jetstream.ErrKeyExists), errors.Is(err, jetstream.ErrBucketExists):
		return gcerrors.AlreadyExists
	case errors.Is(err, jetstream.ErrBadRequest), errors.Is(err, jetstream.ErrInvalidStreamName),
		errors.Is(err, jetstream.ErrInvalidSubject), errors.Is(err, jetstream.ErrInvalidConsumerName),
		errors.Is(err, jetstream.ErrStreamNameRequired), errors.Is(err, jetstream.ErrHandlerRequired),
		errors.Is(err, jetstream.ErrInvalidBucketName), errors.Is(err, jetstream.ErrInvalidKey),
		errors.Is(err, jetstream.ErrInvalidOption), errors.Is(err, jetstream.ErrKeyValueConfigRequired),
		errors.Is(err, jetstream.ErrHistoryTooLarge):
		return gcerrors.InvalidArgument
	case errors.Is(err, jetstream.ErrConsumerAlreadyConsuming), errors.Is(err, jetstream.ErrMsgAlreadyAckd),
		errors.Is(err, jetstream.ErrConsumerHasActiveSubscription), errors.Is(err, jetstream.ErrOrderConsumerUsedAsFetch),
		errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume), errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests):
		return gcerrors.FailedPrecondition
	case errors.Is(err, jetstream.ErrConsumerCreate), errors.Is(err, jetstream.ErrDuplicateFilterSubjects),
		errors.Is(err, jetstream.ErrOverlappingFilterSubjects), errors.Is(err, jetstream.ErrEmptyFilter):
		return gcerrors.Internal
	case errors.Is(err, jetstream.ErrConnectionClosed), errors.Is(err, jetstream.ErrServerShutdown),
		errors.Is(err, jetstream.ErrJetStreamPublisherClosed):
		return gcerrors.Internal
	case errors.Is(err, jetstream.ErrAsyncPublishTimeout):
		return gcerrors.DeadlineExceeded

	default:
		return gcerrors.Unknown
	}
}
