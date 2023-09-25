# jetstreampubsub
A go-cloud pubsub plugin for nats io jetstream.

Package jetstreampubsub provides a pubsub implementation for NATS.io Jetstream. Use OpenTopic to
construct a *pubsub.Topic, and/or OpenSubscription to construct a
*pubsub.Subscription. This package uses gob to encode and decode driver.Message to
[]byte.

# URLs

For pubsub.OpenTopic and pubsub.OpenSubscription, natspubsub registers
for the scheme "nats".
The default URL opener will connect to a default server based on the
environment variable "NATS_SERVER_URL".

For servers that support it (NATS Server 2.2.0 or later), messages can
be encoded using native NATS message headers, and native message content.
This provides full support for non-Go clients. Versions prior to 2.2.0
uses gob.Encoder to encode the message headers and content, which limits
the subscribers only to Go clients.
To use this feature, set the query parameter "natsv2" in the URL.
If no value is provided, it assumes the value is true. Otherwise, the value
needs to be parsable as a boolean. For example:
  - nats://mysubject?natsv2
  - nats://mysubject?natsv2=true

This feature can also be enabled by setting the UseV2 field in the
URLOpener.
If the server does not support this feature, any attempt to use it will
result in an error.
Using native NATS message headers and content is more efficient than using
gob.Encoder, and allows non-Go clients to subscribe to the topic and
receive messages. It is recommended to use this feature if the server
supports it.

To customize the URL opener, or for more details on the URL format,
see URLOpener.
See https://gocloud.dev/concepts/urls/ for background information.

# Message Delivery Semantics

NATS supports at-most-semantics; applications need not call Message.Ack,
and must not call Message.Nack.
See https://godoc.org/gocloud.dev/pubsub#hdr-At_most_once_and_At_least_once_Delivery
for more background.

