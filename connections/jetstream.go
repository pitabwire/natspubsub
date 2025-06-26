package connections

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pitabwire/natspubsub/errorutil"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub/driver"
)

func NewJetstream(natsConn *nats.Conn) (Connection, error) {

	js, err := jetstream.New(natsConn)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "natspubsub: failed to convert connection to jetstream")
	}

	return &jetstreamConnection{jetStream: js}, nil
}

type jetstreamConnection struct {
	// Connection to use for communication with the server.
	jetStream jetstream.JetStream
}

func (c *jetstreamConnection) Close() error {
	// Only drain the underlying connection if no one else needs it. It may be being used by other components
	// Actual connection cleanup should is managed at a higher level, using the Connector implementation in URLOpener
	if c == nil || c.jetStream == nil {
		return nil
	}

	conn := c.jetStream.Conn()
	// Drain the connection if we have one
	if conn != nil {
		return conn.Drain()
	}

	return nil
}

func (c *jetstreamConnection) Raw() interface{} {
	return c.jetStream
}

func (c *jetstreamConnection) CreateTopic(ctx context.Context, opts *TopicOptions, connector Connector) (Topic, error) {

	if opts.StreamConfig.Name != "" {

		stream, err := c.jetStream.Stream(ctx, opts.StreamConfig.Name)
		if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, errorutil.Wrap(err, gcerrors.Internal, "failed to get stream")
		}

		if stream == nil {
			_, err = c.jetStream.CreateOrUpdateStream(ctx, opts.StreamConfig)
			if err != nil {
				return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to create or update stream %s", opts.StreamConfig.Name)
			}
		}
	}

	return &jetstreamTopic{subject: opts.Subject, subjectExtHeader: opts.HeaderExtendingSubject, jetStream: c.jetStream, connector: connector}, nil
}

func (c *jetstreamConnection) CreateSubscription(ctx context.Context, opts *SubscriptionOptions, connector Connector) (Queue, error) {

	stream, err := c.jetStream.Stream(ctx, opts.StreamConfig.Name)
	if err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to get stream %s", opts.StreamConfig.Name)
	}

	if stream == nil {
		stream, err = c.jetStream.CreateOrUpdateStream(ctx, opts.StreamConfig)
		if err != nil {
			return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to create or update stream %s", opts.StreamConfig.Name)
		}
	}

	// Create durable consumer
	consumer, err := stream.CreateOrUpdateConsumer(ctx, opts.ConsumerConfig)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "failed to create or update consumer %s", opts.ConsumerConfig.Name)
	}

	return newJetstreamConsumer(connector, consumer, opts.ReceiveWaitTimeOut), nil

}

func (c *jetstreamConnection) DeleteSubscription(ctx context.Context, opts *SubscriptionOptions) error {
	err := c.jetStream.DeleteConsumer(ctx, opts.StreamConfig.Name, opts.ConsumerConfig.Name)
	if err != nil {
		return errorutil.Wrapf(err, gcerrors.Internal, "failed to delete consumer %s from stream %s", opts.ConsumerConfig.Name, opts.StreamConfig.Name)
	}
	return nil
}

type jetstreamTopic struct {
	subject          string
	subjectExtHeader string
	jetStream        jetstream.JetStream
	connector        Connector
}

func (t *jetstreamTopic) Close() error {
	// Nothing to close for the jetstreamTopic as the underlying JetStream connection
	// is managed by the jetstreamConnection and should be closed there
	if t == nil || t.connector == nil {
		return nil
	}

	return t.connector.ConfirmClose()
}

func (t *jetstreamTopic) Encode(dm *driver.Message) (*nats.Msg, error) {
	subject := t.Subject()
	if t.subjectExtHeader != "" {
		subject = subject + subjectExtension(t.subjectExtHeader, dm.Metadata)
	}
	return encodeMessage(dm, subject)
}
func (t *jetstreamTopic) Subject() string {
	return t.subject
}

func (t *jetstreamTopic) PublishMessage(ctx context.Context, msg *nats.Msg) (string, error) {

	ack, err := t.jetStream.PublishMsg(ctx, msg)
	if err != nil {
		return "", errorutil.Wrapf(err, gcerrors.Internal, "failed to publish message to subject %s", msg.Subject)
	}
	return createLoggableID(ack.Stream, ack.Sequence), nil
}

type jetstreamConsumer struct {
	connector Connector

	consumer    jetstream.Consumer
	activeBatch jetstream.MessageBatch
	mu          sync.Mutex

	pullWaitTimeout time.Duration
}

func newJetstreamConsumer(connector Connector, consumer jetstream.Consumer, pullWaitTimeout time.Duration) *jetstreamConsumer {
	return &jetstreamConsumer{
		connector:       connector,
		consumer:        consumer,
		pullWaitTimeout: pullWaitTimeout,
	}
}

func (jc *jetstreamConsumer) Close() error {
	// We don't have direct access to close the consumer since the consumer is managed by
	// the stream. Instead, we should make sure all pending messages have been properly
	// acknowledged before closing.

	// Return nil as the actual consumer lifetime is managed by the JetStream server
	// and its configuration (TTL, interest, etc.)
	if jc == nil || jc.connector == nil {
		return nil
	}
	return jc.connector.ConfirmClose()
}

func (jc *jetstreamConsumer) CanNack() bool {
	return true
}

func (jc *jetstreamConsumer) Unsubscribe() error {
	return nil
}

func (jc *jetstreamConsumer) setupActiveBatch(ctx context.Context, batchCount int, batchTimeout time.Duration) (jetstream.MessageBatch, error) {
	jc.mu.Lock()
	defer jc.mu.Unlock()

	if jc.activeBatch != nil {
		return jc.activeBatch, nil
	}

	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Canceled, "context canceled while setting up batch")
	}

	// Use Fetch to block for extended periods
	// This provides better behaviour when there are no messages available
	batch, err := jc.consumer.Fetch(batchCount, jetstream.FetchMaxWait(batchTimeout))
	if err != nil {
		// Map connection-related errors
		if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrConnectionDraining) {
			return nil, errorutil.Wrap(err, gcerrors.ResourceExhausted, "connection issue while setting up batch")
		}
		return nil, errorutil.Wrap(err, gcerrors.Internal, "failed to setup fetch from consumer")
	}

	jc.activeBatch = batch
	return jc.activeBatch, nil
}

func (jc *jetstreamConsumer) clearActiveBatch() {
	jc.mu.Lock()
	jc.activeBatch = nil
	jc.mu.Unlock()
}

func (jc *jetstreamConsumer) pullMessages(ctx context.Context, batchCount int, batchTimeout time.Duration) ([]*driver.Message, error) {
	messages := make([]*driver.Message, 0, batchCount)

	activeBatch, err := jc.setupActiveBatch(ctx, batchCount, batchTimeout)
	if err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Internal, "active batch is nil")
	}

	for {
		select {
		case <-ctx.Done():
			// If we already have messages, return them instead of error
			if len(messages) > 0 {
				return messages, nil
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, errorutil.Wrap(ctx.Err(), gcerrors.DeadlineExceeded, "timeout while waiting for messages")
			}
			return messages, errorutil.Wrap(ctx.Err(), gcerrors.Canceled, "context canceled while processing messages")

		case msg, ok := <-activeBatch.Messages():
			if !ok {
				// Channel closed, we've processed all messages
				// Let setupActiveBatch manage the activeBatch field
				jc.clearActiveBatch()
				return messages, nil
			}

			err = activeBatch.Error()
			if err != nil {
				// Clear the batch on error to allow retry on next call
				jc.clearActiveBatch()
				return messages, errorutil.Wrap(err, gcerrors.Internal, "batch fetch error")
			}

			drvMsg, err0 := decodeJsMessage(msg)
			if err0 != nil {
				continue
			}
			messages = append(messages, drvMsg)

			metadata, err0 := msg.Metadata()
			if err0 != nil {
				continue
			}

			if len(messages) >= batchCount || metadata.NumPending == 0 {
				return messages, nil
			}
		}
	}
}

func (jc *jetstreamConsumer) ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Canceled, "context canceled")
	}

	if batchCount <= 0 {
		batchCount = 1
	}

	// Pull messages
	messages, err := jc.pullMessages(ctx, batchCount, jc.pullWaitTimeout)

	// Special handling for no messages case
	if err == nil && len(messages) == 0 {
		// This is an acceptable condition - no messages available
		return messages, nil
	}

	return messages, err
}

func (jc *jetstreamConsumer) Ack(ctx context.Context, ids []driver.AckID) error {

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil
		}

		msg, ok := id.(jetstream.Msg)
		if !ok {
			continue
		}

		_ = msg.Ack()
	}

	return nil
}

func (jc *jetstreamConsumer) Nack(ctx context.Context, ids []driver.AckID) error {

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil
		}

		msg, ok := id.(jetstream.Msg)
		if !ok {
			continue
		}

		_ = msg.Nak()
	}

	return nil
}

func jsMessageAsFunc(msg jetstream.Msg) func(interface{}) bool {
	return func(i interface{}) bool {
		if p, ok := i.(*jetstream.Msg); ok {
			*p = msg
			return true
		}

		return false
	}
}

func decodeJsMessage(msg jetstream.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, errorutil.Wrap(nats.ErrInvalidMsg, gcerrors.InvalidArgument, "invalid message: nil message")
	}

	dm := &driver.Message{
		AsFunc: jsMessageAsFunc(msg),
		Body:   msg.Data(),
		AckID:  msg,
	}

	h := msg.Headers()

	if h != nil {
		dm.Metadata = make(map[string]string, len(h))

		for k, v := range h {
			var sv string
			if len(v) > 0 {
				sv = v[0]
			}

			decodedKey, err0 := url.QueryUnescape(k)
			if err0 != nil {
				decodedKey = k
			}

			decodedValue, err0 := url.QueryUnescape(sv)
			if err0 != nil {
				decodedValue = sv
			}

			dm.Metadata[decodedKey] = decodedValue
		}
	}

	md, err := msg.Metadata()
	if err == nil {
		dm.LoggableID = createLoggableID(md.Stream, md.Sequence.Stream)
	}

	return dm, nil
}

func createLoggableID(streamName string, streamID uint64) string {
	return fmt.Sprintf("%s/%d", streamName, streamID)
}
