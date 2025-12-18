package connections

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gnatsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	testServerUrlFmt = "nats://127.0.0.1:%d"
	testPort         = 11223
)

type testHarness struct {
	server *server.Server
	nc     *nats.Conn
}

func newPlainTestHarness(t *testing.T) *testHarness {
	opts := gnatsd.DefaultTestOptions
	opts.Port = testPort
	s := gnatsd.RunServer(&opts)

	nc, err := nats.Connect(fmt.Sprintf(testServerUrlFmt, testPort))
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	return &testHarness{server: s, nc: nc}
}

func newJetstreamTestHarness(t *testing.T) *testHarness {
	opts := gnatsd.DefaultTestOptions
	opts.Port = testPort
	opts.JetStream = true
	s := gnatsd.RunServer(&opts)

	nc, err := nats.Connect(fmt.Sprintf(testServerUrlFmt, testPort))
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	return &testHarness{server: s, nc: nc}
}

func (h *testHarness) Close() {
	if h.nc != nil {
		h.nc.Close()
	}
	if h.server != nil {
		h.server.Shutdown()
	}
}

// mockConnector implements Connector for testing
type mockConnector struct {
	conn Connection
}

func (m *mockConnector) Connection() Connection {
	return m.conn
}

func (m *mockConnector) ConfirmClose() error {
	return nil
}

func (m *mockConnector) ConfirmOpen() int32 {
	return 0
}

// TestPlainNatsTopicAs tests the As() method on plainNatsTopic
func TestPlainNatsTopicAs(t *testing.T) {
	h := newPlainTestHarness(t)
	defer h.Close()

	conn, err := NewPlain(h.nc)
	if err != nil {
		t.Fatalf("failed to create plain connection: %v", err)
	}

	connector := &mockConnector{conn: conn}
	ctx := context.Background()

	topic, err := conn.CreateTopic(ctx, &TopicOptions{Subject: "test.topic"}, connector)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	t.Run("Connector", func(t *testing.T) {
		var c Connector
		if !topic.As(&c) {
			t.Error("As(&Connector) should return true")
		}
		if c != connector {
			t.Error("As(&Connector) should set the connector")
		}
	})

	t.Run("Connection", func(t *testing.T) {
		var c Connection
		if !topic.As(&c) {
			t.Error("As(&Connection) should return true")
		}
		if c != conn {
			t.Error("As(&Connection) should set the connection")
		}
	})

	t.Run("NatsConn", func(t *testing.T) {
		var nc *nats.Conn
		if !topic.As(&nc) {
			t.Error("As(**nats.Conn) should return true")
		}
		if nc != h.nc {
			t.Error("As(**nats.Conn) should set the nats connection")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		var s string
		if topic.As(&s) {
			t.Error("As(&string) should return false")
		}
	})

	t.Run("NonPointer", func(t *testing.T) {
		var c Connector
		if topic.As(c) {
			t.Error("As(Connector) non-pointer should return false")
		}
	})

	t.Run("Nil", func(t *testing.T) {
		if topic.As(nil) {
			t.Error("As(nil) should return false")
		}
	})
}

// TestNatsConsumerAs tests the As() method on natsConsumer
func TestNatsConsumerAs(t *testing.T) {
	h := newPlainTestHarness(t)
	defer h.Close()

	conn, err := NewPlain(h.nc)
	if err != nil {
		t.Fatalf("failed to create plain connection: %v", err)
	}

	connector := &mockConnector{conn: conn}
	ctx := context.Background()

	queue, err := conn.CreateSubscription(ctx, &SubscriptionOptions{
		Subject:            "test.subscription",
		ReceiveWaitTimeOut: 100 * time.Millisecond,
	}, connector)
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer queue.Close()

	t.Run("NatsSubscription", func(t *testing.T) {
		var sub *nats.Subscription
		if !queue.As(&sub) {
			t.Error("As(**nats.Subscription) should return true")
		}
		if sub == nil {
			t.Error("As(**nats.Subscription) should set a non-nil subscription")
		}
	})

	t.Run("NatsConn", func(t *testing.T) {
		var nc *nats.Conn
		if !queue.As(&nc) {
			t.Error("As(**nats.Conn) should return true")
		}
		if nc != h.nc {
			t.Error("As(**nats.Conn) should set the nats connection")
		}
	})

	t.Run("Connector", func(t *testing.T) {
		var c Connector
		if !queue.As(&c) {
			t.Error("As(&Connector) should return true")
		}
		if c != connector {
			t.Error("As(&Connector) should set the connector")
		}
	})

	t.Run("Connection", func(t *testing.T) {
		var c Connection
		if !queue.As(&c) {
			t.Error("As(&Connection) should return true")
		}
		if c != conn {
			t.Error("As(&Connection) should set the connection")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		var s string
		if queue.As(&s) {
			t.Error("As(&string) should return false")
		}
	})

	t.Run("NonPointer", func(t *testing.T) {
		var c Connector
		if queue.As(c) {
			t.Error("As(Connector) non-pointer should return false")
		}
	})

	t.Run("Nil", func(t *testing.T) {
		if queue.As(nil) {
			t.Error("As(nil) should return false")
		}
	})
}

// TestJetstreamTopicAs tests the As() method on jetstreamTopic
func TestJetstreamTopicAs(t *testing.T) {
	h := newJetstreamTestHarness(t)
	defer h.Close()

	conn, err := NewJetstream(h.nc)
	if err != nil {
		t.Fatalf("failed to create jetstream connection: %v", err)
	}

	connector := &mockConnector{conn: conn}
	ctx := context.Background()

	topic, err := conn.CreateTopic(ctx, &TopicOptions{Subject: "test.topic"}, connector)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	t.Run("Connector", func(t *testing.T) {
		var c Connector
		if !topic.As(&c) {
			t.Error("As(&Connector) should return true")
		}
		if c != connector {
			t.Error("As(&Connector) should set the connector")
		}
	})

	t.Run("Connection", func(t *testing.T) {
		var c Connection
		if !topic.As(&c) {
			t.Error("As(&Connection) should return true")
		}
		if c != conn {
			t.Error("As(&Connection) should set the connection")
		}
	})

	t.Run("JetStream", func(t *testing.T) {
		var js jetstream.JetStream
		if !topic.As(&js) {
			t.Error("As(*jetstream.JetStream) should return true")
		}
		if js == nil {
			t.Error("As(*jetstream.JetStream) should set a non-nil JetStream")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		var s string
		if topic.As(&s) {
			t.Error("As(&string) should return false")
		}
	})

	t.Run("NonPointer", func(t *testing.T) {
		var c Connector
		if topic.As(c) {
			t.Error("As(Connector) non-pointer should return false")
		}
	})

	t.Run("Nil", func(t *testing.T) {
		if topic.As(nil) {
			t.Error("As(nil) should return false")
		}
	})
}

// TestJetstreamConsumerAs tests the As() method on jetstreamConsumer
func TestJetstreamConsumerAs(t *testing.T) {
	h := newJetstreamTestHarness(t)
	defer h.Close()

	conn, err := NewJetstream(h.nc)
	if err != nil {
		t.Fatalf("failed to create jetstream connection: %v", err)
	}

	connector := &mockConnector{conn: conn}
	ctx := context.Background()

	queue, err := conn.CreateSubscription(ctx, &SubscriptionOptions{
		Subject:            "test.js.subscription",
		ReceiveWaitTimeOut: 100 * time.Millisecond,
		StreamConfig: jetstream.StreamConfig{
			Name:      "teststream",
			Subjects:  []string{"test.js.subscription"},
			Retention: jetstream.InterestPolicy,
			Storage:   jetstream.MemoryStorage,
		},
		ConsumerConfig: jetstream.ConsumerConfig{
			Name:          "testconsumer",
			FilterSubject: "test.js.subscription",
		},
	}, connector)
	if err != nil {
		t.Fatalf("failed to create subscription: %v", err)
	}
	defer queue.Close()

	t.Run("Connector", func(t *testing.T) {
		var c Connector
		if !queue.As(&c) {
			t.Error("As(&Connector) should return true")
		}
		if c != connector {
			t.Error("As(&Connector) should set the connector")
		}
	})

	t.Run("Connection", func(t *testing.T) {
		var c Connection
		if !queue.As(&c) {
			t.Error("As(&Connection) should return true")
		}
		if c != conn {
			t.Error("As(&Connection) should set the connection")
		}
	})

	t.Run("JetstreamConsumer", func(t *testing.T) {
		var consumer jetstream.Consumer
		if !queue.As(&consumer) {
			t.Error("As(*jetstream.Consumer) should return true")
		}
		if consumer == nil {
			t.Error("As(*jetstream.Consumer) should set a non-nil Consumer")
		}
	})

	t.Run("JetStream", func(t *testing.T) {
		var js jetstream.JetStream
		if !queue.As(&js) {
			t.Error("As(*jetstream.JetStream) should return true")
		}
		if js == nil {
			t.Error("As(*jetstream.JetStream) should set a non-nil JetStream")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		var s string
		if queue.As(&s) {
			t.Error("As(&string) should return false")
		}
	})

	t.Run("NonPointer", func(t *testing.T) {
		var c Connector
		if queue.As(c) {
			t.Error("As(Connector) non-pointer should return false")
		}
	})

	t.Run("Nil", func(t *testing.T) {
		if queue.As(nil) {
			t.Error("As(nil) should return false")
		}
	})
}

// TestQueueGroupConsumerAs tests the As() method on natsConsumer with queue group
func TestQueueGroupConsumerAs(t *testing.T) {
	h := newPlainTestHarness(t)
	defer h.Close()

	conn, err := NewPlain(h.nc)
	if err != nil {
		t.Fatalf("failed to create plain connection: %v", err)
	}

	connector := &mockConnector{conn: conn}
	ctx := context.Background()

	queue, err := conn.CreateSubscription(ctx, &SubscriptionOptions{
		Subject:            "test.queue.subscription",
		ReceiveWaitTimeOut: 100 * time.Millisecond,
		ConsumerConfig: jetstream.ConsumerConfig{
			Durable: "testqueue",
		},
	}, connector)
	if err != nil {
		t.Fatalf("failed to create queue subscription: %v", err)
	}
	defer queue.Close()

	t.Run("NatsSubscription", func(t *testing.T) {
		var sub *nats.Subscription
		if !queue.As(&sub) {
			t.Error("As(**nats.Subscription) should return true for queue group")
		}
		if sub == nil {
			t.Error("As(**nats.Subscription) should set a non-nil subscription")
		}
	})

	t.Run("Connector", func(t *testing.T) {
		var c Connector
		if !queue.As(&c) {
			t.Error("As(&Connector) should return true")
		}
	})

	t.Run("Connection", func(t *testing.T) {
		var c Connection
		if !queue.As(&c) {
			t.Error("As(&Connection) should return true")
		}
	})
}
