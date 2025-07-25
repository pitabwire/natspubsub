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

//
// natspubsub exposes the following types for use:
//   - Conn: *nats.Conn
//   - Subscription: *nats.Subscription
//   - Message.BeforeSend: *nats.Msg for v2.
//   - Message.AfterSend: None.
//   - Message: *nats.Msg
//
//	This implementation does not support nats version 1.0, actually from nats v2.2 onwards only.
//
//

package natspubsub

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pitabwire/natspubsub/connections"
	"github.com/pitabwire/natspubsub/errorutil"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

const (
	QueryParameterNatsV1    = "nats_v1"
	QueryParameterJetstream = "jetstream"

	QueryParamSubject               = "subject"
	QueryParamHeaderToExtendSubject = "header_to_extended_subject"
	QueryParamReceiveWaitTimeout    = "receive_wait_timeout"
	DefaultReceiveWaitTimeout       = 30 * time.Second

	ReceiveBatchConfigPrefix = "receive_batch_"
	AckBatchConfigPrefix     = "ack_batch_"
	ConsumerConfigPrefix     = "consumer_"
	StreamConfigPrefix       = "stream_"
)

var defaultURIParameters = []string{QueryParameterNatsV1, QueryParameterJetstream, QueryParamSubject, QueryParamHeaderToExtendSubject, QueryParamReceiveWaitTimeout}
var BatchReceiveURIParameters = []string{
	"receive_batch_max_handlers", "receive_batch_min_batch_size", "receive_batch_max_batch_size", "receive_batch_max_batch_byte_size",
}
var BatchAckURIParameters = []string{"ack_batch_max_handlers", "ack_batch_min_batch_size", "ack_batch_max_batch_size", "ack_batch_max_batch_byte_size"}
var StreamURIParameters = []string{"stream_name", "stream_description", "stream_subjects", "stream_retention", "stream_max_consumers",
	"stream_max_msgs", "stream_max_bytes", "stream_discard", "stream_discard_new_per_subject", "stream_max_age",
	"stream_max_msgs_per_subject", "stream_max_msg_size", "stream_storage", "stream_num_replicas", "stream_no_ack",
	"stream_duplicate_window", "stream_placement", "stream_mirror", "stream_sources", "stream_sealed",
	"stream_deny_delete", "stream_deny_purge", "stream_allow_rollup_hdrs", "stream_compression", "stream_first_seq",
	"stream_subject_transform", "stream_republish", "stream_allow_direct", "stream_mirror_direct", "stream_consumer_limits",
	"stream_metadata", "stream_template_owner", "stream_allow_msg_ttl", "stream_subject_delete_marker_ttl",
}
var ConsumerURIParameters = []string{
	"consumer_name", "consumer_durable_name", "consumer_description", "consumer_deliver_policy",
	"consumer_opt_start_seq", "consumer_opt_start_time", "consumer_ack_policy", "consumer_ack_wait",
	"consumer_max_deliver", "consumer_backoff", "consumer_filter_subject", "consumer_replay_policy",
	"consumer_rate_limit_bps", "consumer_sample_freq", "consumer_max_waiting", "consumer_max_ack_pending",
	"consumer_headers_only", "consumer_max_batch", "consumer_max_expires", "consumer_max_bytes",
	"consumer_inactive_threshold", "consumer_num_replicas", "consumer_mem_storage", "consumer_filter_subjects",
	"consumer_metadata", "consumer_pause_until", "consumer_priority_policy", "consumer_priority_timeout",
	"consumer_priority_groups",
}
var allowedParameters []string // Start with the first slice

var errNotSubjectInitialized = errorutil.New(gcerrors.NotFound, "natspubsub: subject not initialised")
var errDuplicateParameter = errorutil.New(gcerrors.InvalidArgument, "natspubsub: avoid specifying parameters more than once")
var errNotSupportedParameter = errorutil.Newf(gcerrors.InvalidArgument, "natspubsub: unsupported parameter used, supported parameters include [ %s ]", strings.Join(allowedParameters, ", "))

func init() {

	o := new(defaultDialer)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, o)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, o)

	allowedParameters = append(allowedParameters, defaultURIParameters...)
	allowedParameters = append(allowedParameters, BatchReceiveURIParameters...)
	allowedParameters = append(allowedParameters, BatchAckURIParameters...)
	allowedParameters = append(allowedParameters, StreamURIParameters...)
	allowedParameters = append(allowedParameters, ConsumerURIParameters...)
}

// defaultDialer dials a NATS server based on the provided url
// see: https://docs.nats.io/using-nats/developer/connecting
// Guidance :
//   - This dialer will only use the url formart nats://...
//   - The dialer stores a map of unique nats connections without the parameters
type defaultDialer struct {
	mutex sync.Mutex

	openerMap sync.Map
}

// defaultConn
func (o *defaultDialer) defaultConn(_ context.Context, serverUrl *url.URL) (*URLOpener, error) {

	o.mutex.Lock()
	defer o.mutex.Unlock()

	connectionUrl := strings.Replace(serverUrl.String(), serverUrl.RequestURI(), "", 1)

	for param, values := range serverUrl.Query() {
		paramName := strings.ToLower(param)
		if !slices.Contains(allowedParameters, paramName) {
			return nil, errNotSupportedParameter
		}

		if len(values) != 1 {
			return nil, errDuplicateParameter
		}
	}

	storedOpener, ok := o.openerMap.Load(connectionUrl)
	if ok {
		opener := storedOpener.(*URLOpener)
		opener.ConfirmOpen()
		return opener, nil
	}

	useV1Encoding := o.featureIsEnabledViaUrl(serverUrl.Query(), QueryParameterNatsV1)
	isJetstreamEnabled := o.featureIsEnabledViaUrl(serverUrl.Query(), QueryParameterJetstream)

	conn, err := o.createConnection(connectionUrl, isJetstreamEnabled, useV1Encoding)
	if err != nil {
		return nil, err
	}

	opener := &URLOpener{
		Conn:                conn,
		TopicOptions:        connections.TopicOptions{},
		SubscriptionOptions: connections.SubscriptionOptions{},
		refCount:            &atomic.Int32{},
		dialer:              o,
		connectionURL:       connectionUrl,
	}

	// Initialise reference count to 1
	opener.refCount.Store(1)

	o.openerMap.Store(connectionUrl, opener)

	return opener, nil
}

func (o *defaultDialer) createConnection(connectionUrl string, isJetstreamEnabled bool, useV1Encoding bool) (connections.Connection, error) {
	natsConn, err := nats.Connect(connectionUrl)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.FailedPrecondition, "failed to dial server using %q", connectionUrl)
	}

	sv, err := connections.ServerVersion(natsConn.ConnectedServerVersion())
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, " failed to parse NATS server version %q", natsConn.ConnectedServerVersion())
	}

	var conn connections.Connection

	if !sv.JetstreamSupported() || !isJetstreamEnabled {
		conn, err = connections.NewPlainWithEncodingV1(natsConn, useV1Encoding)
	} else {
		conn, err = connections.NewJetstream(natsConn)
	}

	if err != nil {
		return nil, errorutil.Wrap(err, gcerrors.Internal, "natspubsub: failed to create connection")
	}

	return conn, nil
}

func (o *defaultDialer) featureIsEnabledViaUrl(q url.Values, key string) bool {
	if len(q) == 0 {
		return false
	}
	v, ok := q[key]
	if !ok {
		return false
	}

	if len(v) == 0 {
		// If the query parameter was provided without any value i.e. nats://mysubject?natsv2
		// it assumes the value is true.
		return true
	}
	if len(v) > 1 {
		return false
	}
	if v[0] == "" {
		return true
	}
	useV2, err := strconv.ParseBool(v[0])
	if err != nil {
		return false
	}
	return useV2
}

func (o *defaultDialer) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opener, err := o.defaultConn(ctx, u)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "open topic %v: failed to open default connection", u)
	}
	return opener.OpenTopicURL(ctx, u)
}

func (o *defaultDialer) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opener, err := o.defaultConn(ctx, u)
	if err != nil {
		return nil, errorutil.Wrapf(err, gcerrors.Internal, "open subscription %v: failed to open default connection", u)
	}
	return opener.OpenSubscriptionURL(ctx, u)
}

// cleanupOpener properly cleans up and removes an opener from the openerMap
func (o *defaultDialer) cleanupOpener(connectionURL string) error {

	val, ok := o.openerMap.Load(connectionURL)
	if !ok {
		return nil
	}

	opener, ok := val.(*URLOpener)
	if !ok {
		return nil
	}

	// Extract the underlying NATS connection based on the type
	conn := opener.Connection()
	err := conn.Close()

	// Remove from the map
	o.openerMap.Delete(connectionURL)
	return err
}

// Scheme is the URL scheme natspubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "nats"

// URLOpener opens NATS URLs like "nats://mysubject".
//
// The URL host+path is used as the subject.
//
// No query parameters are supported.
type URLOpener struct {
	Conn connections.Connection
	// TopicOptions specifies the options to pass to OpenTopic.
	TopicOptions connections.TopicOptions
	// SubscriptionOptions specifies the options to pass to OpenSubscription.
	SubscriptionOptions connections.SubscriptionOptions

	// Reference counting with atomic operations
	refCount      *atomic.Int32
	dialer        *defaultDialer
	connectionURL string
}

// Connection increments the reference count of topics and subscriptions opened
func (o *URLOpener) Connection() connections.Connection {
	return o.Conn
}

// ConfirmOpen increments the reference count of topics and subscriptions opened
func (o *URLOpener) ConfirmOpen() int32 {
	return o.refCount.Add(1)
}

// ConfirmClose decrements the reference count and returns the new count
func (o *URLOpener) ConfirmClose() error {
	activeCount := o.refCount.Add(-1)
	if activeCount <= 0 {
		return o.dialer.cleanupOpener(o.connectionURL)
	}
	return nil
}

func cleanSubjectFromUrl(u *url.URL) (string, error) {
	subject := u.Query().Get(QueryParamSubject)

	// Clean the leading slash from the path
	pathPart := strings.TrimPrefix(u.Path, "/")

	if pathPart != "" {
		if subject == "" {
			subject = pathPart
		} else {
			subject += "." + pathPart
		}
	}

	if subject == "" && !u.Query().Has(QueryParameterJetstream) {
		return "", errNotSubjectInitialized
	}

	return subject, nil
}

func cleanSettingValue(key, val string) any {
	val = strings.TrimSpace(val)

	if key == "backoff" {
		var backOff []time.Duration
		for _, v := range strings.Split(val, ",") {
			i, err := time.ParseDuration(v)
			if err != nil {
				return val
			}
			backOff = append(backOff, i)
		}
		return backOff
	} else if slices.Contains([]string{"ack_wait", "max_expires", "inactive_threshold", "priority_timeout", "duplicate_window", "subject_delete_marker_ttl"}, key) {
		i, err := time.ParseDuration(val)
		if err != nil {
			return val
		}
		return i
	} else if strings.HasPrefix(key, "max_") || strings.HasPrefix(key, "num_") || slices.Contains([]string{"duplicate_window", "first_seq", "opt_start_seq", "replay_policy", "rate_limit_bps", "inactive_threshold", "priority_timeout"}, key) {
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return val
		}
		return i
	} else if slices.Contains([]string{"mem_storage", "headers_only", "discard_new_per_subject", "no_ack", "sealed", "deny_delete", "deny_purge", "allow_rollup_hdrs", "allow_direct", "mirror_direct", "allow_msg_ttl"}, key) {
		i, err := strconv.ParseBool(val)
		if err != nil {
			return val
		}
		return i
	} else if slices.Contains([]string{"subjects", "filter_subjects", "priority_groups"}, key) {
		return strings.Split(val, ",")
	}

	return val
}

// OpenTopicURL opens a pubsub.Topic based on a url supplied.
//
//	A topic can be specified in the subject and suffixed by the url path
//	These definitions will yield the subject shown infront of them
//
//		- nats://host:8934?subject=foo --> foo
//		- nats://host:8934/bar?subject=foo --> foo/bar
//		- nats://host:8934/bar --> /bar
//		- nats://host:8934?no_subject=foo --> [this yields an error]
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {

	opts := &o.TopicOptions

	var err error
	opts.Subject, err = cleanSubjectFromUrl(u)
	if err != nil {
		return nil, err
	}

	queryParams := u.Query()

	opts.HeaderExtendingSubject = queryParams.Get(QueryParamHeaderToExtendSubject)

	streamMap := make(map[string]any)
	for key, values := range queryParams {
		// Use the first value if multiple values exist for a key
		if len(values) == 0 {
			continue
		}

		configVal := values[0]
		if strings.HasPrefix(key, StreamConfigPrefix) {
			streamMap[strings.TrimPrefix(key, StreamConfigPrefix)] = cleanSettingValue(strings.TrimPrefix(key, StreamConfigPrefix), configVal)
		}
	}

	jsonData, err := json.Marshal(streamMap)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonData, &opts.StreamConfig)
	if err != nil {
		return nil, err
	}

	return OpenTopic(ctx, o, opts)
}

// OpenSubscriptionURL opens a pubsub.Subscription based on url supplied.
//
//	 A subscription also creates the required underlaying queue or streams
//	 There are many more parameters checked in this case compared to the publish topic section.
//	 If required, the list of parameters can be extended, but for now only a subset is defined and
//	 the remaining ones utilise the sensible defaults that nats come with.
//
//		The list of parameters includes:
//
//			- subject,
//			- stream_name,
//			- stream_description,
//			- stream_subjects,
//			- consumer_max_count,
//			- consumer_queue
//			- consumer_max_waiting

func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {

	opts := &o.SubscriptionOptions

	var err error
	opts.Subject, err = cleanSubjectFromUrl(u)
	if err != nil {
		return nil, err
	}

	queryParams := u.Query()

	opts.ReceiveWaitTimeOut, err = time.ParseDuration(queryParams.Get(QueryParamReceiveWaitTimeout))
	if err != nil {
		opts.ReceiveWaitTimeOut = DefaultReceiveWaitTimeout
	}

	streamMap := make(map[string]any)
	consumerMap := make(map[string]any)
	receiveBatchMap := make(map[string]any)
	ackBatchMap := make(map[string]any)
	for key, values := range queryParams {
		// Use the first value if multiple values exist for a key
		if len(values) == 0 {
			continue
		}

		configVal := values[0]
		if strings.HasPrefix(key, StreamConfigPrefix) {
			streamMap[strings.TrimPrefix(key, StreamConfigPrefix)] = cleanSettingValue(strings.TrimPrefix(key, StreamConfigPrefix), configVal)
		} else if strings.HasPrefix(key, ConsumerConfigPrefix) {
			consumerMap[strings.TrimPrefix(key, ConsumerConfigPrefix)] = cleanSettingValue(strings.TrimPrefix(key, ConsumerConfigPrefix), configVal)
		} else if strings.HasPrefix(key, ReceiveBatchConfigPrefix) {
			receiveBatchMap[strings.TrimPrefix(key, ReceiveBatchConfigPrefix)] = cleanSettingValue(strings.TrimPrefix(key, ReceiveBatchConfigPrefix), configVal)
		} else if strings.HasPrefix(key, AckBatchConfigPrefix) {
			ackBatchMap[strings.TrimPrefix(key, AckBatchConfigPrefix)] = cleanSettingValue(strings.TrimPrefix(key, AckBatchConfigPrefix), configVal)
		}
	}

	jsonData, err := json.Marshal(streamMap)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonData, &opts.StreamConfig)
	if err != nil {
		return nil, err
	}

	jsonData, err = json.Marshal(consumerMap)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonData, &opts.ConsumerConfig)
	if err != nil {
		return nil, err
	}

	jsonData, err = json.Marshal(receiveBatchMap)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonData, &opts.ReceiveBatchConfig)
	if err != nil {
		return nil, err
	}

	jsonData, err = json.Marshal(ackBatchMap)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonData, &opts.AckBatchConfig)
	if err != nil {
		return nil, err
	}

	return OpenSubscription(ctx, o, opts)

}

type topic struct {
	iTopic connections.Topic
}

// OpenTopic returns a *pubsub.Topic for use with NATS, version 2.2.0 and above is recommended.
// This changes the encoding of the message as, starting with version 2.2.0, NATS supports message headers.
// In previous versions the message headers were encoded along with the message content using gob.Encoder,
// which limits the subscribers only to Go clients.
// This implementation uses native NATS message headers, and native message content, which provides full support
// for non-Go clients.
func OpenTopic(ctx context.Context, connector connections.Connector, opts *connections.TopicOptions) (*pubsub.Topic, error) {
	dt, err := openTopic(ctx, connector, opts)
	if err != nil {
		return nil, err
	}
	return pubsub.NewTopic(dt, nil), nil
}

// openTopic returns the driver for OpenTopic. This function exists so the test
// harness can get the driver interface implementation if it needs to.
func openTopic(ctx context.Context, connector connections.Connector, opts *connections.TopicOptions) (driver.Topic, error) {

	conn := connector.Connection()
	t, err := conn.CreateTopic(ctx, opts, connector)
	if err != nil {
		return nil, err
	}

	return &topic{iTopic: t}, nil
}

// SendBatch implements driver.Conn.SendBatch.
func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	if t == nil || t.iTopic == nil {
		return errNotSubjectInitialized
	}

	for _, m := range msgs {

		err := ctx.Err()
		if err != nil {
			return err
		}

		err = t.sendMessage(ctx, m)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *topic) sendMessage(ctx context.Context, m *driver.Message) error {
	var msg *nats.Msg
	var err error

	msg, err = t.iTopic.Encode(m)
	if err != nil {
		return err
	}

	if m.BeforeSend != nil {
		asFunc := func(i any) bool {
			if nm, ok := i.(**nats.Msg); ok {
				*nm = msg
				return true
			}
			return false
		}
		if err = m.BeforeSend(asFunc); err != nil {
			return err
		}
	}

	sentId, err := t.iTopic.PublishMessage(ctx, msg)
	if err != nil {
		return err
	}

	if m.AfterSend != nil {
		asFunc := func(i any) bool {
			if p, ok := i.(*string); ok {
				*p = sentId
				return true
			}
			return false
		}
		err = m.AfterSend(asFunc)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsRetryable implements driver.Conn.IsRetryable.
func (t *topic) IsRetryable(error) bool { return false }

// As implements driver.Conn.As.
func (t *topic) As(i interface{}) bool {
	c, ok := i.(*connections.Topic)
	if !ok {
		return false
	}
	*c = t.iTopic
	return true
}

// ErrorAs implements driver.Conn.ErrorAs
func (t *topic) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Conn.ErrorCode
func (t *topic) ErrorCode(err error) gcerrors.ErrorCode {

	// Use our error mapping function for all other cases
	return errorutil.MapErrorCode(err)
}

// Close implements driver.Conn.Close.
func (t *topic) Close() error {
	if t == nil || t.iTopic == nil {
		return nil
	}
	// First close the actual topic
	return t.iTopic.Close()
}

type subscription struct {
	queue  connections.Queue
	opener *URLOpener
	url    string
}

// OpenSubscription returns a *pubsub.Subscription representing a NATS subscription
// or NATS queue subscription for use with NATS at least version 2.2.0.
// This changes the encoding of the message as, starting with version 2.2.0, NATS supports message headers.
// In previous versions the message headers were encoded along with the message content using gob.Encoder,
// which limits the subscribers only to Go clients.
// This implementation uses native NATS message headers, and native message content, which provides full support
// for non-Go clients.
func OpenSubscription(ctx context.Context, connector connections.Connector, opts *connections.SubscriptionOptions) (*pubsub.Subscription, error) {
	ds, err := openSubscription(ctx, connector, opts)
	if err != nil {
		return nil, err
	}

	return pubsub.NewSubscription(ds, opts.ReceiveBatchConfig.To(), opts.AckBatchConfig.To()), nil
}

func openSubscription(ctx context.Context, connector connections.Connector, opts *connections.SubscriptionOptions) (driver.Subscription, error) {
	if opts == nil {
		return nil, errors.New("natspubsub: subscription options missing")
	}

	conn := connector.Connection()

	q, err := conn.CreateSubscription(ctx, opts, connector)
	if err != nil {
		return nil, err
	}

	// Get URL and opener from conn if possible
	var opener *URLOpener
	var url string

	// Try to extract the opener from the connection context if available
	if conn, ok := conn.(interface{ GetOpenerInfo() (*URLOpener, string) }); ok {
		opener, url = conn.GetOpenerInfo()
	}

	return &subscription{queue: q, opener: opener, url: url}, nil
}

// ReceiveBatch implements driver.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, batchCount int) ([]*driver.Message, error) {

	if s == nil || s.queue == nil {
		return nil, nats.ErrBadSubscription
	}

	return s.queue.ReceiveMessages(ctx, batchCount)
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ids []driver.AckID) error {
	// Ack is a no-op.
	return s.queue.Ack(ctx, ids)
}

// CanNack implements driver.CanNack.
func (s *subscription) CanNack() bool {
	if s == nil || s.queue == nil {
		return false
	}
	return s.queue.CanNack()
}

// SendNacks implements driver.Subscription.SendNacks
func (s *subscription) SendNacks(ctx context.Context, ids []driver.AckID) error {
	return s.queue.Nack(ctx, ids)
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (s *subscription) IsRetryable(error) bool { return false }

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {

	if p, ok := i.(*connections.Queue); ok {
		*p = s.queue
		return true
	}

	return false

}

// ErrorAs implements driver.Subscription.ErrorAs
func (*subscription) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Subscription.ErrorCode
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	// Use our error mapping function for all other cases
	return errorutil.MapErrorCode(err)
}

// Close implements driver.Subscription.Close.
func (s *subscription) Close() error {
	if s == nil || s.queue == nil {
		return nil
	}
	// First close the actual subscription
	return s.queue.Close()
}
