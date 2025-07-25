package connections

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"net/url"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go"
	"github.com/pitabwire/natspubsub/errorutil"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub/driver"
)

// bufferPool provides reusable byte buffers for encoding/decoding operations
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func NewPlainWithEncodingV1(natsConn *nats.Conn, useV1Encoding bool) (Connection, error) {
	sv, err := ServerVersion(natsConn.ConnectedServerVersion())
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to parse server version: %s", natsConn.ConnectedServerVersion())
	}

	return newPlainConnection(natsConn, sv, useV1Encoding), nil
}

func NewPlain(natsConn *nats.Conn) (Connection, error) {
	return NewPlainWithEncodingV1(natsConn, false)
}

func newPlainConnection(natsConn *nats.Conn, version *Version, useV1Encoding bool) Connection {
	return &plainConnection{natsConnection: natsConn, version: version, useV1Encoding: useV1Encoding}
}

type plainConnection struct {
	// Connection to use for communication with the server.
	natsConnection *nats.Conn
	useV1Encoding  bool
	version        *Version
}

func (c *plainConnection) Close() error {
	// Only drain the underlying connection if no one else needs it. It may be being used by other components
	// Actual connection cleanup should is managed at a higher level, using the Connector implementation in URLOpener
	if c == nil || c.natsConnection == nil {
		return nil
	}

	// Drain the connection if we have one
	return c.natsConnection.Drain()

}

func (c *plainConnection) Raw() interface{} {
	return c.natsConnection
}

func (c *plainConnection) CreateTopic(ctx context.Context, opts *TopicOptions, connector Connector) (Topic, error) {

	useV1Encoding := !c.version.V2Supported() || c.useV1Encoding
	return &plainNatsTopic{subject: opts.Subject, subjectExtHeader: opts.HeaderExtendingSubject, plainConn: c.natsConnection, useV1Encoding: useV1Encoding, connector: connector}, nil
}

func (c *plainConnection) CreateSubscription(ctx context.Context, opts *SubscriptionOptions, connector Connector) (Queue, error) {

	// We force the batch fetch size to 1, as only jetstream enabled connections can do batch fetches
	// see: https://pkg.go.dev/github.com/nats-io/nats.go@v1.30.1#Conn.QueueSubscribeSync
	opts.ConsumerConfig.MaxRequestBatch = 1

	// Determine if we should use V1 encoding - either we need to because server doesn't support V2
	// or the client explicitly requested V1 encoding
	useV1Decoding := !c.version.V2Supported() || c.useV1Encoding

	if opts.ConsumerConfig.Durable != "" {

		subsc, err := c.natsConnection.QueueSubscribeSync(opts.Subject, opts.ConsumerConfig.Durable)
		if err != nil {
			return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to subscribe to queue %s on subject %s",
				opts.ConsumerConfig.Durable, opts.Subject)
		}

		return &natsConsumer{consumer: subsc, isQueueGroup: true,
			batchFetchTimeout: opts.ConsumerConfig.MaxRequestExpires,
			useV1Decoding:     useV1Decoding}, nil
	}

	// Using nats without any form of queue mechanism is fine only where
	// loosing some messages is ok as this essentially is an atmost once delivery situation here.
	subsc, err := c.natsConnection.SubscribeSync(opts.Subject)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to subscribe to subject %s", opts.Subject)
	}

	return &natsConsumer{consumer: subsc, isQueueGroup: false,
		batchFetchTimeout: opts.ConsumerConfig.MaxRequestExpires,
		useV1Decoding:     useV1Decoding, connector: connector}, nil

}

func (c *plainConnection) DeleteSubscription(ctx context.Context, opts *SubscriptionOptions) error {
	return nil
}

type plainNatsTopic struct {
	subject          string
	subjectExtHeader string
	plainConn        *nats.Conn
	useV1Encoding    bool
	connector        Connector
}

func (t *plainNatsTopic) Close() error {
	// Nothing specific to close for plainNatsTopic
	// The underlying NATS connection is managed by the plainConnection
	if t == nil || t.connector == nil {
		return nil
	}

	return t.connector.ConfirmClose()
}

func (t *plainNatsTopic) Encode(dm *driver.Message) (*nats.Msg, error) {

	subject := t.Subject()
	if t.subjectExtHeader != "" {
		subject = subject + subjectExtension(t.subjectExtHeader, dm.Metadata)
	}

	if t.useV1Encoding {
		return encodeV1Message(dm, subject)
	}
	return encodeMessage(dm, subject)
}
func (t *plainNatsTopic) Subject() string {
	return t.subject
}
func (t *plainNatsTopic) PublishMessage(_ context.Context, msg *nats.Msg) (string, error) {
	var err error
	if t.useV1Encoding {
		err = t.plainConn.Publish(msg.Subject, msg.Data)
		if err != nil {
			return "", errorutil.Wrapf(err, gcerrors.Internal, "failed to publish message to subject %s", msg.Subject)
		}
		return "", nil
	}
	err = t.plainConn.PublishMsg(msg)
	if err != nil {
		return "", errorutil.Wrapf(err, gcerrors.Internal, "failed to publish message to subject %s", msg.Subject)
	}
	return "", nil
}

type natsConsumer struct {
	consumer          *nats.Subscription
	isQueueGroup      bool
	batchFetchTimeout time.Duration
	useV1Decoding     bool
	connector         Connector
}

func (q *natsConsumer) Close() error {

	if q == nil || q.connector == nil {
		return nil
	}

	return q.connector.ConfirmClose()
}

func (q *natsConsumer) CanNack() bool {
	return false
}

func (q *natsConsumer) UseV1Decoding() bool {
	return q.useV1Decoding
}

func (q *natsConsumer) Unsubscribe() error {
	return q.consumer.Unsubscribe()
}

func (q *natsConsumer) ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error) {
	// Pre-allocate message slice with capacity of batchCount to reduce allocations
	messages := make([]*driver.Message, 0, batchCount)

	if batchCount <= 0 {
		batchCount = 1
	}

	// Use the context's deadline if available, otherwise fall back to the configured timeout
	fetchTimeout := q.batchFetchTimeout
	if deadline, ok := ctx.Deadline(); ok {
		// Use the remaining time from the context, but don't exceed our configured timeout
		remainingTime := time.Until(deadline)
		if remainingTime < fetchTimeout {
			fetchTimeout = remainingTime
		}
		// Ensure we have at least a minimal timeout to prevent spinning
		if fetchTimeout <= 0 {
			fetchTimeout = time.Millisecond
		}
	}

	// Try to fetch up to batchCount messages without blocking too long on any single message
	// This helps prevent deadlocks while still attempting to fill the batch
	for i := 0; i < batchCount; i++ {
		// Check if context is done before attempting to fetch each message
		if err := ctx.Err(); err != nil {
			return messages, errorutil.Wrap(err, gcerrors.Canceled, "context canceled while receiving messages")
		}

		// Calculate timeout for this fetch attempt (use shorter timeouts for subsequent messages)
		attemptTimeout := fetchTimeout
		if i > 0 {
			// Use progressively shorter timeouts for subsequent messages
			// This enables quick return when no more messages are available
			attemptTimeout = fetchTimeout / time.Duration(i*2+1)
			if attemptTimeout < time.Millisecond {
				attemptTimeout = time.Millisecond
			}
		}

		msg, err := q.consumer.NextMsg(attemptTimeout)
		if err != nil {
			// Not an error if we timeout or context is cancelled
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				// Just return what we have so far
				return messages, nil
			}
			// For other errors, stop and return the error
			return messages, errorutil.Wrap(err, gcerrors.Internal, "error receiving message")
		}

		var driverMsg *driver.Message
		if q.UseV1Decoding() {
			driverMsg, err = decodeV1Message(msg)
		} else {
			driverMsg, err = decodeMessage(msg)
		}

		if err != nil {
			return nil, errorutil.Wrap(err, gcerrors.Internal, "error decoding message")
		}

		messages = append(messages, driverMsg)
	}

	return messages, nil
}

func (q *natsConsumer) Ack(_ context.Context, _ []driver.AckID) error {
	// Just do nothing as plain nats does not have ack semantics
	// In plain NATS, messages are fire-and-forget by default
	// There’s no persistence and no built-in acknowledgment mechanism.
	return nil
}

func (q *natsConsumer) Nack(_ context.Context, _ []driver.AckID) error {
	// Just do nothing
	return nil
}

func messageAsFunc(msg *nats.Msg) func(interface{}) bool {
	return func(i any) bool {
		p, ok := i.(**nats.Msg)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}

func decodeV1Message(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, errorutil.Wrap(nats.ErrInvalidMsg, gcerrors.InvalidArgument, "invalid message: nil message")
	}

	dm := &driver.Message{}
	dm.AsFunc = messageAsFunc(msg)

	// Try to decode as a v1 encoded message (with metadata and body encoded using gob)
	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()
	buf.Write(msg.Data)
	dec := gob.NewDecoder(buf)

	// Estimate initial metadata map size to minimise allocations
	metadata := make(map[string]string, 8) // A reasonable starting size for most messages
	if err := dec.Decode(&metadata); err != nil {
		// If we can't decode as v1 format, treat the entire payload as body
		dm.Metadata = nil
		dm.Body = msg.Data
		return dm, nil
	}
	dm.Metadata = metadata

	// Now decode the body
	var body []byte
	if err := dec.Decode(&body); err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Internal, "failed to decode message body")
	}
	dm.Body = body

	return dm, nil
}

func decodeMessage(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, errorutil.Wrap(nats.ErrInvalidMsg, gcerrors.InvalidArgument, "invalid message: nil message")
	}

	dm := driver.Message{
		AsFunc: messageAsFunc(msg),
		Body:   msg.Data,
	}

	if msg.Header != nil {
		// Pre-allocate metadata map with the expected capacity
		dm.Metadata = make(map[string]string, len(msg.Header))
		for k, v := range msg.Header {
			var sv string
			if len(v) > 0 {
				sv = v[0]
			}
			// Decode URL-encoded key and value
			decodedKey, err := url.QueryUnescape(k)
			if err != nil {
				decodedKey = k // Fallback to original if decoding fails
			}

			decodedValue, err := url.QueryUnescape(sv)
			if err != nil {
				decodedValue = sv // Fallback to original if decoding fails
			}

			dm.Metadata[decodedKey] = decodedValue
		}
	}

	dm.AckID = msg

	return &dm, nil
}

func encodeV1Message(dm *driver.Message, sub string) (*nats.Msg, error) {

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	// Always encode metadata, even if empty - this ensures consistent message format
	if err := enc.Encode(dm.Metadata); err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Internal, "failed to encode message metadata")
	}
	if err := enc.Encode(dm.Body); err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Internal, "failed to encode message body")
	}
	return &nats.Msg{
		Subject: sub,
		Data:    buf.Bytes(),
	}, nil

}

func encodeMessage(dm *driver.Message, sub string) (*nats.Msg, error) {
	var header nats.Header
	if dm.Metadata != nil {
		header = nats.Header{}
		for k, v := range dm.Metadata {

			if !utf8.ValidString(k) {
				return nil, errorutil.Newf(gcerrors.InvalidArgument, "pubsub: Message.Metadata keys must be valid UTF-8 strings: %q", k)
			}
			if !utf8.ValidString(v) {
				return nil, errorutil.Newf(gcerrors.InvalidArgument, "pubsub: Message.Metadata values must be valid UTF-8 strings: %q", v)
			}

			// URL-encode key and value to ensure they are valid header fields
			encodedKey := url.QueryEscape(k)
			encodedValue := url.QueryEscape(v)

			header.Set(encodedKey, encodedValue)
		}
	}

	return &nats.Msg{
		Subject: sub,
		Header:  header,
		Data:    dm.Body,
	}, nil
}
