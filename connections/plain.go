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
	New: func() any {
		return new(bytes.Buffer)
	},
}

func NewPlainWithEncodingV1(natsConn *nats.Conn, useV1Encoding bool) (Connection, error) {
	sv, err := ServerVersion(natsConn.ConnectedServerVersion())
	if err != nil {
		return nil, errorutil.Wrapf(err, "failed to parse server version: %s", natsConn.ConnectedServerVersion())
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

func (c *plainConnection) Raw() any {
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
			return nil, errorutil.Wrapf(err, "failed to subscribe to queue %s on subject %s",
				opts.ConsumerConfig.Durable, opts.Subject)
		}

		return &natsConsumer{consumer: subsc, isQueueGroup: true,
			batchFetchTimeout: opts.ReceiveWaitTimeOut,
			useV1Decoding:     useV1Decoding, connector: connector}, nil
	}

	// Using nats without any form of queue mechanism is fine only where
	// loosing some messages is ok as this essentially is an atmost once delivery situation here.
	subsc, err := c.natsConnection.SubscribeSync(opts.Subject)
	if err != nil {
		return nil, errorutil.Wrapf(err, "failed to subscribe to subject %s", opts.Subject)
	}

	return &natsConsumer{consumer: subsc, isQueueGroup: false,
		batchFetchTimeout: opts.ReceiveWaitTimeOut,
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

func (t *plainNatsTopic) As(i any) bool {
	if p, ok := i.(*Connector); ok {
		*p = t.connector
		return true
	}

	if p, ok := i.(*Connection); ok {
		*p = t.connector.Connection()
		return true
	}

	if p, ok := i.(**nats.Conn); ok {
		*p = t.plainConn
		return true
	}

	return false
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
			return "", errorutil.Wrapf(err, "failed to publish message to subject %s", msg.Subject)
		}
		return "", nil
	}
	err = t.plainConn.PublishMsg(msg)
	if err != nil {
		return "", errorutil.Wrapf(err, "failed to publish message to subject %s", msg.Subject)
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

func (q *natsConsumer) As(i any) bool {

	if p, ok := i.(**nats.Subscription); ok {
		*p = q.consumer
		return true
	}

	if p, ok := i.(**nats.Conn); ok {
		*p = q.connector.Connection().(*plainConnection).natsConnection
		return true
	}

	if p, ok := i.(*Connector); ok {
		*p = q.connector
		return true
	}

	if p, ok := i.(*Connection); ok {
		*p = q.connector.Connection()
		return true
	}

	return false
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

const (
	minFetchTimeout   = time.Millisecond
	subsequentTimeout = 10 * time.Millisecond
)

func (q *natsConsumer) ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error) {
	if batchCount <= 0 {
		batchCount = 1
	}

	messages := make([]*driver.Message, 0, batchCount)

	// Calculate effective timeout respecting context deadline
	fetchTimeout := q.batchFetchTimeout
	if fetchTimeout > 0 {
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < fetchTimeout {
				fetchTimeout = remaining
			}
			if fetchTimeout < minFetchTimeout {
				fetchTimeout = minFetchTimeout
			}
		}
	}

	for i := 0; i < batchCount; i++ {
		if err := ctx.Err(); err != nil {
			if len(messages) > 0 {
				return messages, nil
			}
			return nil, errorutil.Wrap(err, "context canceled while receiving messages")
		}

		var (
			msg *nats.Msg
			err error
		)

		if fetchTimeout == 0 && i == 0 {
			msg, err = q.consumer.NextMsgWithContext(ctx)
		} else {
			// First message uses full timeout; subsequent use short poll
			attemptTimeout := fetchTimeout
			if i > 0 {
				attemptTimeout = subsequentTimeout
			}
			if attemptTimeout < minFetchTimeout {
				attemptTimeout = minFetchTimeout
			}
			msg, err = q.consumer.NextMsg(attemptTimeout)
		}

		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				return messages, nil
			}
			if len(messages) > 0 {
				return messages, nil
			}
			return nil, errorutil.Wrap(err, "error receiving message")
		}

		var driverMsg *driver.Message
		if q.UseV1Decoding() {
			driverMsg, err = decodeV1Message(msg)
		} else {
			driverMsg, err = decodeMessage(msg)
		}

		if err != nil {
			return messages, errorutil.Wrap(err, "error decoding message")
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

func messageAsFunc(msg *nats.Msg) func(any) bool {
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
		return nil, errorutil.New(gcerrors.InvalidArgument, "invalid message: nil message")
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
		return nil, errorutil.Wrap(err, "failed to decode message body")
	}
	dm.Body = body

	return dm, nil
}

func decodeMessage(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, errorutil.New(gcerrors.InvalidArgument, "invalid message: nil message")
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
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	enc := gob.NewEncoder(buf)
	if err := enc.Encode(dm.Metadata); err != nil {
		bufferPool.Put(buf)
		return nil, errorutil.Wrap(err, "failed to encode message metadata")
	}
	if err := enc.Encode(dm.Body); err != nil {
		bufferPool.Put(buf)
		return nil, errorutil.Wrap(err, "failed to encode message body")
	}

	// Copy data before returning buffer to pool
	data := make([]byte, buf.Len())
	copy(data, buf.Bytes())
	bufferPool.Put(buf)

	return &nats.Msg{
		Subject: sub,
		Data:    data,
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
