// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// # As
//
// jetstreampubsub exposes the following types for As:
//   - Topic: *nats.Conn
//   - Subscription: *nats.Subscription
//   - Message.BeforeSend: None for v1, *nats.Msg for v2.
//   - Message.AfterSend: None.
//   - Message: *nats.Msg

package jetstreampubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

var errNotInitialized = errors.New("natspubsub: topic not initialized")

var recvBatcherOpts = &batcher.Options{
	// NATS has at-most-once semantics, meaning once it delivers a message, the
	// message won't be delivered again.
	// Therefore, we can't allow the portable type to read-ahead and queue any
	// messages; they might end up undelivered if the user never calls Receive
	// to get them. Setting both the MaxBatchSize and MaxHandlers to one means
	// that we'll only return a message at a time, which should be immediately
	// returned to the user.
	//
	// Note: there is a race condition where the original Receive that
	// triggered a call to ReceiveBatch ends up failing (e.g., due to a
	// Done context), and ReceiveBatch returns a message that ends up being
	// queued for the next Receive. That message is at risk of being dropped.
	// This seems OK.
	MaxBatchSize: 1,
	MaxHandlers:  1, // max concurrency for receives
}

func init() {
	o := new(defaultDialer)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, o)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, o)
}

// defaultDialer dials a default NATS server based on the environment
// variable "NATS_SERVER_URL".
type defaultDialer struct {
	init sync.Once
	err  error

	opener URLOpener
}

func (o *defaultDialer) defaultConn(ctx context.Context) error {
	o.init.Do(func() {
		serverURL := os.Getenv("NATS_SERVER_URL")
		if serverURL == "" {
			o.err = errors.New("NATS_SERVER_URL environment variable not set")
			return
		}
		conn, err := nats.Connect(serverURL)
		if err != nil {
			o.err = fmt.Errorf("failed to dial NATS_SERVER_URL %q: %v", serverURL, err)
			return
		}

		js, err := jetstream.New(conn)
		if err != nil {
			o.err = fmt.Errorf("failed to make server jetstream : %v", err)
			return
		}

		o.opener = URLOpener{Stream: js}
	})
	return o.err
}

type serverVersion struct {
	major, minor, patch int
}

func (o *defaultDialer) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	err := o.defaultConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open topic %v: failed to open default connection: %v", u, err)
	}
	return o.opener.OpenTopicURL(ctx, u)
}

func (o *defaultDialer) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	err := o.defaultConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open subscription %v: failed to open default connection: %v", u, err)
	}
	return o.opener.OpenSubscriptionURL(ctx, u)
}

var semVerRegexp = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)

func parseServerVersion(version string) (serverVersion, error) {
	m := semVerRegexp.FindStringSubmatch(version)
	if m == nil {
		return serverVersion{}, errors.New("failed to parse server version")
	}
	var (
		major, minor, patch int
		err                 error
	)
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return serverVersion{}, fmt.Errorf("failed to parse server version major number %q: %v", m[1], err)
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return serverVersion{}, fmt.Errorf("failed to parse server version minor number %q: %v", m[2], err)
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return serverVersion{}, fmt.Errorf("failed to parse server version patch number %q: %v", m[3], err)
	}
	return serverVersion{major: major, minor: minor, patch: patch}, nil
}

// Scheme is the URL scheme natspubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "natsjs"

// URLOpener opens NATS URLs like "nats://mysubject?natsv2=true".
//
// The URL host+path is used as the subject.
//
// No query parameters are supported.
type URLOpener struct {
	// Connection to use for communication with the server.
	Stream jetstream.JetStream
	// TopicOptions specifies the options to pass to OpenTopic.
	TopicOptions TopicOptions
	// SubscriptionOptions specifies the options to pass to OpenSubscription.
	SubscriptionOptions SubscriptionOptions
	// UseV2 indicates whether the NATS Server is at least version 2.2.0.
	UseV2 bool
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	for param := range u.Query() {
		switch strings.ToLower(param) {
		case "queue":
		default:

			return nil, fmt.Errorf("open topic %v: invalid query parameter %s", u, param)
		}
	}
	subject := path.Join(u.Host, u.Path)
	return OpenTopic(o.Stream, subject, &o.TopicOptions)

}

// OpenSubscriptionURL opens a pubsub.Subscription based on u.
func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opts := o.SubscriptionOptions
	for param, values := range u.Query() {
		switch strings.ToLower(param) {
		case "subject":
			opts.StreamSubjects = values
		case "queue":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: many consumer queues set", u)
			}
			opts.ConsumerQueue = values[0]
		case "client_name":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: many client names specified", u)
			}
			opts.ConsumerName = values[0]
		case "max_consumers":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: many max consumers set ", u)
			}

			maxConsumers, err := strconv.Atoi(values[0])
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: max consumers should be an int  %s", u, param)
			}

			opts.MaxConsumers = maxConsumers

		default:
			return nil, fmt.Errorf("open subscription %v: invalid query parameter %s", u, param)
		}
	}
	subject := path.Join(u.Host, u.Path)
	return OpenSubscription(ctx, o.Stream, subject, &opts)

}

// TopicOptions sets options for constructing a *pubsub.Topic backed by NATS.
type TopicOptions struct{}

// SubscriptionOptions sets options for constructing a *pubsub.Subscription
// backed by NATS.
type SubscriptionOptions struct {
	StreamSubjects []string

	ConsumerName  string
	ConsumerQueue string

	MaxConsumers int
}

type topic struct {
	js   jetstream.JetStream
	subj string
}

// OpenTopic returns a *pubsub.Topic for use with NATS at least version 2.2.0.
// This changes the encoding of the message as, starting with version 2.2.0, NATS supports message headers.
// In previous versions the message headers were encoded along with the message content using gob.Encoder,
// which limits the subscribers only to Go clients.
// This implementation uses native NATS message headers, and native message content, which provides full support
// for non-Go clients.
func OpenTopic(js jetstream.JetStream, subject string, _ *TopicOptions) (*pubsub.Topic, error) {
	dt, err := openTopic(js, subject)
	if err != nil {
		return nil, err
	}
	return pubsub.NewTopic(dt, nil), nil
}

// openTopic returns the driver for OpenTopic. This function exists so the test
// harness can get the driver interface implementation if it needs to.
func openTopic(js jetstream.JetStream, subject string) (driver.Topic, error) {
	if js == nil {
		return nil, errors.New("jetstream: JetStream is required")
	}

	return &topic{js: js, subj: subject}, nil
}

// SendBatch implements driver.Topic.SendBatch.
func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	if t == nil || t.js == nil {
		return errNotInitialized
	}

	for _, m := range msgs {
		err := ctx.Err()
		if err != nil {
			return err
		}

		err = t.sendMessageV2(ctx, m)
		if err != nil {
			return err
		}
	}
	// Per specification this is supposed to only return after
	// a message has been sent. Normally NATS is very efficient
	// at sending messages in batches on its own and also handles
	// disconnected buffering during a reconnect event. We will
	// let NATS handle this for now. If needed we could add a
	// FlushWithContext() call which ensures the connected server
	// has processed all the messages.
	return nil
}

func (t *topic) sendMessageV2(ctx context.Context, m *driver.Message) error {
	msg := encodeMessageV2(m, t.subj)
	if m.BeforeSend != nil {
		asFunc := func(i interface{}) bool {
			if nm, ok := i.(**nats.Msg); ok {
				*nm = msg
				return true
			}
			return false
		}
		if err := m.BeforeSend(asFunc); err != nil {
			return err
		}
	}

	if _, err := t.js.PublishMsg(ctx, msg); err != nil {
		return err
	}

	if m.AfterSend != nil {
		asFunc := func(i interface{}) bool { return false }
		if err := m.AfterSend(asFunc); err != nil {
			return err
		}
	}
	return nil
}

// IsRetryable implements driver.Topic.IsRetryable.
func (*topic) IsRetryable(error) bool { return false }

// As implements driver.Topic.As.
func (t *topic) As(i interface{}) bool {
	c, ok := i.(*jetstream.JetStream)
	if !ok {
		return false
	}
	*c = t.js
	return true
}

// ErrorAs implements driver.Topic.ErrorAs
func (*topic) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Topic.ErrorCode
func (*topic) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case err == nil:
		return gcerrors.OK
	case errors.Is(err, context.Canceled):
		return gcerrors.Canceled
	case errors.Is(err, errNotInitialized):
		return gcerrors.NotFound
	case errors.Is(err, nats.ErrBadSubject):
		return gcerrors.FailedPrecondition
	case errors.Is(err, nats.ErrAuthorization):
		return gcerrors.PermissionDenied
	case errors.Is(err, nats.ErrMaxPayload), errors.Is(err, nats.ErrReconnectBufExceeded):
		return gcerrors.ResourceExhausted
	}
	return gcerrors.Unknown
}

// Close implements driver.Topic.Close.
func (*topic) Close() error { return nil }

type subscription struct {
	js jetstream.JetStream

	stream   jetstream.Stream
	consumer jetstream.Consumer
}

// OpenSubscription returns a *pubsub.Subscription representing a NATS subscription or NATS queue subscription
// for use with NATS at least version 2.2.0.
// This changes the encoding of the message as, starting with version 2.2.0, NATS supports message headers.
// In previous versions the message headers were encoded along with the message content using gob.Encoder,
// which limits the subscribers only to Go clients.
// This implementation uses native NATS message headers, and native message content, which provides full support
// for non-Go clients.
func OpenSubscription(ctx context.Context, js jetstream.JetStream, subject string, opts *SubscriptionOptions) (*pubsub.Subscription, error) {
	ds, err := openSubscription(ctx, js, subject, opts)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(ds, recvBatcherOpts, nil), nil
}

func openSubscription(ctx context.Context, js jetstream.JetStream, streamName string, opts *SubscriptionOptions) (driver.Subscription, error) {
	var err error
	if opts == nil {
		return nil, errors.New("jetstream: Subscription options missing")
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	if stream == nil {

		streamConfig := jetstream.StreamConfig{
			Name:         streamName,
			Subjects:     opts.StreamSubjects,
			MaxConsumers: opts.MaxConsumers,
		}

		stream, err = js.CreateStream(ctx, streamConfig)
		if err != nil {
			return nil, err
		}

	}

	// Create durable consumer
	c, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:      opts.ConsumerName,
		Durable:   opts.ConsumerQueue,
		AckPolicy: jetstream.AckExplicitPolicy,
	})

	if err != nil {
		return nil, err
	}
	return &subscription{js: js, stream: stream, consumer: c}, nil
}

// ReceiveBatch implements driver.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, count int) ([]*driver.Message, error) {

	if s == nil || s.consumer == nil {
		return nil, nats.ErrBadSubscription
	}

	var messages []*driver.Message

	if count <= 1 {
		msg, err := s.consumer.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				return nil, nil
			}
			return nil, err
		}

		driverMsg, err := decodeMessageV2(msg)

		if err != nil {
			return nil, err
		}

		messages = append(messages, driverMsg)
	} else {
		msgBatch, err := s.consumer.Fetch(count, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				return nil, nil
			}
			return nil, err
		}

		for msg := range msgBatch.Messages() {

			driverMsg, err0 := decodeMessageV2(msg)

			if err0 != nil {
				return nil, err0
			}

			messages = append(messages, driverMsg)

		}
	}

	return messages, nil
}

func messageAsFunc(msg jetstream.Msg) func(interface{}) bool {
	return func(i any) bool {
		p, ok := i.(*jetstream.Msg)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ids []driver.AckID) error {
	// Ack is a no-op.
	return nil
}

// CanNack implements driver.CanNack.
func (s *subscription) CanNack() bool { return false }

// SendNacks implements driver.Subscription.SendNacks. It should never be called
// because we return false for CanNack.
func (s *subscription) SendNacks(ctx context.Context, ids []driver.AckID) error {
	panic("unreachable")
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (s *subscription) IsRetryable(error) bool { return false }

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	c, ok := i.(*jetstream.Consumer)
	if !ok {
		return false
	}
	*c = s.consumer
	return true
}

// ErrorAs implements driver.Subscription.ErrorAs
func (*subscription) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Subscription.ErrorCode
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	switch {
	case err == nil:
		return gcerrors.OK
	case errors.Is(err, context.Canceled):
		return gcerrors.Canceled
	case errors.Is(err, errNotInitialized), errors.Is(err, nats.ErrBadSubscription):
		return gcerrors.NotFound
	case errors.Is(err, nats.ErrBadSubject), errors.Is(err, nats.ErrTypeSubscription):
		return gcerrors.FailedPrecondition
	case errors.Is(err, nats.ErrAuthorization):
		return gcerrors.PermissionDenied
	case errors.Is(err, nats.ErrMaxMessages), errors.Is(err, nats.ErrSlowConsumer):
		return gcerrors.ResourceExhausted
	case errors.Is(err, nats.ErrTimeout):
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}

// Close implements driver.Subscription.Close.
func (*subscription) Close() error { return nil }

func encodeMessageV2(dm *driver.Message, sub string) *nats.Msg {
	var header nats.Header
	if dm.Metadata != nil {
		header = nats.Header{}
		for k, v := range dm.Metadata {
			header[url.QueryEscape(k)] = []string{url.QueryEscape(v)}
		}
	}
	return &nats.Msg{
		Subject: sub,
		Data:    dm.Body,
		Header:  header,
	}
}

func decodeMessageV2(msg jetstream.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}

	dm := driver.Message{
		AsFunc: messageAsFunc(msg),
		Body:   msg.Data(),
	}

	if msg.Headers() != nil {
		dm.Metadata = map[string]string{}
		for k, v := range msg.Headers() {
			var sv string
			if len(v) > 0 {
				sv = v[0]
			}
			kb, err := url.QueryUnescape(k)
			if err != nil {
				return nil, err
			}
			vb, err := url.QueryUnescape(sv)
			if err != nil {
				return nil, err
			}
			dm.Metadata[kb] = vb
		}
	}

	return &dm, nil
}
