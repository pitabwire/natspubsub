package connections

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
	"regexp"
	"strconv"
)

type Version struct {
	Major, Minor, Patch int
}

func (v Version) JetstreamSupported() bool {
	return v.Major >= 2 && v.Minor >= 2
}

func (v Version) V2Supported() bool {
	return v.Major >= 2
}

// TopicOptions sets options for constructing a *pubsub.Topic backed by NATS.
type TopicOptions struct {
	Subject string
}

// SubscriptionOptions sets options for subscribing to NATS.
// Stream Options are useful for initial setup especially when creating streams/queues
// these will later be subscribed to by the consumers of nats messages.
// The appropriate *pubsub.Subscription is created as a result here.
type SubscriptionOptions struct {
	Subject string

	BatchConfig    batcher.Options
	StreamConfig   jetstream.StreamConfig
	ConsumerConfig jetstream.ConsumerConfig
}

type Queue interface {
	UseV1Decoding() bool
	// ReceiveMessages pulls messages from the nats queue server.
	// If no messages are currently available, this method should block for
	// no more than about 1 second. It can return an empty
	// slice of messages and no error. ReceiveBatch will be called again
	// immediately, so implementations should try to wait for messages for some
	// non-zero amount of time before returning zero messages. If the underlying
	// service doesn't support waiting, then a time.Sleep can be used.
	ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error)
	Unsubscribe() error
	Ack(ctx context.Context, ids []driver.AckID) error
	Nack(ctx context.Context, ids []driver.AckID) error
	IsQueueGroup() bool
}

type Topic interface {
	UseV1Encoding() bool
	Subject() string
	PublishMessage(ctx context.Context, msg *nats.Msg) (string, error)
}

type Connection interface {
	Raw() interface{}
	CreateSubscription(ctx context.Context, opts *SubscriptionOptions) (Queue, error)
	CreateTopic(ctx context.Context, opts *TopicOptions) (Topic, error)
	DeleteSubscription(ctx context.Context, opts *SubscriptionOptions) error
}

var semVerRegexp = regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)

func ServerVersion(version string) (*Version, error) {
	m := semVerRegexp.FindStringSubmatch(version)
	if m == nil {
		return nil, errors.New("failed to parse server version")
	}
	var (
		major, minor, patch int
		err                 error
	)
	major, err = strconv.Atoi(m[1])
	if err != nil {
		return nil, fmt.Errorf("failed to parse server version major number %q: %v", m[1], err)
	}
	minor, err = strconv.Atoi(m[2])
	if err != nil {
		return nil, fmt.Errorf("failed to parse server version minor number %q: %v", m[2], err)
	}
	patch, err = strconv.Atoi(m[3])
	if err != nil {
		return nil, fmt.Errorf("failed to parse server version patch number %q: %v", m[3], err)
	}
	return &Version{Major: major, Minor: minor, Patch: patch}, nil
}
