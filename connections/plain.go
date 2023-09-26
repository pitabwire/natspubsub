package connections

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"gocloud.dev/pubsub/driver"
	"net/url"
	"time"
)

func NewPlain(natsConn *nats.Conn) ConnectionMux {
	return &natsConnection{natsConnection: natsConn}
}

type natsConnection struct {
	// Connection to use for communication with the server.
	natsConnection *nats.Conn
}

func (c *natsConnection) Raw() interface{} {
	return c.natsConnection
}

func (c *natsConnection) CreateSubscription(ctx context.Context, opts *SubscriptionOptions) (Queue, error) {

	var subsc *nats.Subscription
	var err error

	isDurable := false
	if opts != nil && opts.ConsumerQueue != "" {
		isDurable = true
		subsc, err = c.natsConnection.QueueSubscribeSync(opts.ConsumerSubject, opts.ConsumerQueue)
	} else {
		subsc, err = c.natsConnection.SubscribeSync(opts.ConsumerSubject)
	}
	if err != nil {
		return nil, err
	}

	return &natsConsumer{consumer: subsc, durable: isDurable}, nil

}
func (c *natsConnection) PublishMessage(_ context.Context, msg *nats.Msg) (string, error) {
	var err error
	if err = c.natsConnection.PublishMsg(msg); err != nil {
		return "", err
	}

	return "", nil
}

type natsConsumer struct {
	consumer *nats.Subscription
	durable  bool
}

func (q *natsConsumer) IsDurable() bool {
	return q.durable
}

func (q *natsConsumer) Unsubscribe() error {
	return q.consumer.Unsubscribe()
}

func (q *natsConsumer) ReceiveMessages(ctx context.Context, batchCount int) ([]*driver.Message, error) {

	var messages []*driver.Message

	msgBatch, err := q.consumer.FetchBatch(batchCount, nats.MaxWait(100*time.Millisecond))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		return nil, err
	}

	for msg := range msgBatch.Messages() {

		driverMsg, err0 := decodeMessage(msg)

		if err0 != nil {
			return nil, err0
		}

		messages = append(messages, driverMsg)

	}

	return messages, nil
}

func (q *natsConsumer) Ack(ctx context.Context, ids []driver.AckID) error {
	for _, id := range ids {
		msg, ok := id.(*nats.Msg)
		if !ok {
			continue
		}

		_ = msg.AckSync(nats.Context(ctx))
	}

	return nil
}

func (q *natsConsumer) Nack(ctx context.Context, ids []driver.AckID) error {
	for _, id := range ids {
		msg, ok := id.(*nats.Msg)
		if !ok {
			continue
		}

		_ = msg.Nak(nats.Context(ctx))
	}

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

func decodeMessage(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}

	dm := driver.Message{
		AsFunc: messageAsFunc(msg),
		Body:   msg.Data,
	}

	if msg.Header != nil {
		dm.Metadata = map[string]string{}
		for k, v := range msg.Header {
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

	dm.AckID = msg

	return &dm, nil
}
