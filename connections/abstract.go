package connections

import (
	"context"
	"github.com/nats-io/nats.go"
	"gocloud.dev/pubsub/driver"
)

// TopicOptions sets options for constructing a *pubsub.Topic backed by NATS.
type TopicOptions struct {
	Subject string
}

// SetupOptions sets options utilized especially when creating streams/queues
// these will later be subscribed to by the consumers of nats messages.
type SetupOptions struct {
	StreamName        string
	StreamDescription string

	Subjects     []string
	DurableQueue string
}

// SubscriptionOptions sets options for subscribing to NATS.
// The appropriate *pubsub.Subscription is created as a result here.
type SubscriptionOptions struct {
	ConsumersMaxCount         int
	ConsumerMaxBatchSize      int
	ConsumerMaxBatchBytesSize int
	ConsumerMaxBatchTimeoutMs int

	SetupOpts *SetupOptions
}

type Queue interface {

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
	IsDurable() bool
}

type Topic interface {
	Subject() string
	PublishMessage(ctx context.Context, msg *nats.Msg) (string, error)
}

type Connection interface {
	Raw() interface{}
	CreateSubscription(ctx context.Context, opts *SubscriptionOptions) (Queue, error)
	CreateTopic(ctx context.Context, opts *TopicOptions) (Topic, error)
}
