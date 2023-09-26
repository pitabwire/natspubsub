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
	Subjects          []string
}

// SubscriptionOptions sets options for constructing a *pubsub.Subscription
// backed by NATS.
type SubscriptionOptions struct {
	ConsumerSubject string
	ConsumerQueue   string

	ConsumersMaxCount         int
	ConsumerMaxBatchSize      int
	ConsumerMaxBatchBytesSize int

	SetupOpts *SetupOptions
}

type Queue interface {
	ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error)
	Unsubscribe() error
	Ack(ctx context.Context, ids []driver.AckID) error
	Nack(ctx context.Context, ids []driver.AckID) error
	IsDurable() bool
}

type ConnectionMux interface {
	Raw() interface{}
	CreateSubscription(ctx context.Context, opts *SubscriptionOptions) (Queue, error)

	PublishMessage(ctx context.Context, msg *nats.Msg) (string, error)
}
