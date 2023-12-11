# natspubsub
A go-cloud pubsub plugin for nats io supporting jetstream as well.

[![Tests](https://github.com/pitabwire/natspubsub/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/pitabwire/natspubsub/actions/workflows/tests.yml) 


Package natspubsub provides a pubsub implementation for NATS.io and Jetstream. 
Use OpenTopic to construct a *pubsub.Topic, and/or OpenSubscription to construct a
*pubsub.Subscription. This package uses gob to encode and decode driver.Message to
[]byte.

# URLs

For pubsub.OpenTopic and pubsub.OpenSubscription, natspubsub registers
for the scheme "nats". 

The default URL opener will connect to a default server based on the
environment variable "NATS_SERVER_URL".

This implementation supports (NATS Server 2.2.0 or later), messages can
be encoded using native NATS message headers, and native message content
providing full support for non-Go clients. Operating in this manner is  more 
efficient than using gob.Encoder. Using older versions of the server will result in 
errors and untested scenarios. 

In the event the user cannot upgrade the nats server, we recommend the use of 
the in tree implementation instead: https://github.com/google/go-cloud/tree/master/pubsub/natspubsub

To customize the URL opener, or for more details on the URL format,
see URLOpener.
See https://gocloud.dev/concepts/urls/ for background information.

# Message Delivery Semantics

NATS supports :
See https://godoc.org/gocloud.dev/pubsub#hdr-At_most_once_and_At_least_once_Delivery
for more background.

  1. at-most-once-semantics; applications need not call Message.Ack, applications must also
       not call Message.Nack. 

  2. at-least-once-semantics; this mode is possible with jetstream enabled. 
     applications are supposed to call Message.Ack to remove processed messages from the stream.
     Enabling this the jetstream mode requires adding the jetstream parameter in the url query.


# Subscription options

All the subscription options are passed in as query parameters to the nats url supplied.

Comprehensive definitions of the options can be found here : 
  - https://docs.nats.io/nats-concepts/jetstream/streams
  - https://docs.nats.io/nats-concepts/jetstream/consumers

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Option                           | Default value |                Description 
|----------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| jetstream                        | true          | Enables 
| subject                          |               | A string of characters that form a name which the publisher and subscriber can use to find each other.                                                                                                                                         
| stream_name                      |               | Name of a stream, Names cannot contain whitespace, ., *, >, path separators (forward or backwards slash), and non-printable characters.                                                                                                        
| stream_description               |               | A short explanation of what the stream is about                                                                                                                                                                                                
| stream_subjects                  | []            | A list of subjects to bind. Wildcards are supported. Cannot be set for [mirror](https://docs.nats.io/nats-concepts/jetstream/streams#mirror) streams.                                                                                          
| consumer_max_count               | 10            | How many Consumers can be defined for a given Stream, -1 for unlimited                                                                                                                                                                         
| consumer_durable                 |               | If set, clients can have subscriptions bind to the consumer and resume until the consumer is explicitly deleted. A durable name cannot contain whitespace, ., *, >, path separators (forward or backwards slash), and non-printable characters. 
| consumer_max_waiting             |               | The maximum number of waiting pull requests.
| consumer_max_request_expires_ms  | 30000         | The maximum duration a single pull request will wait for messages to be available to pull.
| consumer_request_batch           | 50            | The maximum batch size a single pull request can make. When set with MaxRequestMaxBytes, the batch size will be constrained by whichever limit is hit first.
| consumer_request_max_batch_bytes | 0             | The maximum total bytes that can be requested in a given batch. When set with MaxRequestBatch, the batch size will be constrained by whichever limit is hit first.
| consumer_request_timeout_ms      | 1000          | Duration the consumer waits for messages to buffer when pulling a batch
| consumer_ack_wait_timeout_ms     | 300000        | The duration that the server will wait for an ack for any individual message once it has been delivered to a consumer. If an ack is not received in time, the message will be redelivered.
| consumer_max_ack_pending         | 100           | Defines the maximum number of messages, without an acknowledgement, that can be outstanding. Once this limit is reached message delivery will be suspended. This limit applies across all of the consumer's bound subscriptions. A value of -1 means there can be any number of pending acks (i.e. no flow control). This does not apply when the AckNone policy is used.



# Publishing options
     
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Option | Default value |                Description 
|--------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Subject       |               | An identifier that publishers send messages to subscribers of interest
