# natspubsub
A go-cloud pubsub plugin for nats io supporting jetstream as well.

[![Tests](https://github.com/pitabwire/natspubsub/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/pitabwire/natspubsub/actions/workflows/tests.yml) 


Package natspubsub provides a pubsub implementation for NATS.io and Jetstream. 
Use OpenTopic to construct a *pubsub.Topic, and/or OpenSubscription to construct a
*pubsub.Subscription. This package uses gob to encode and decode driver.Message to
[]byte.

# Usage (publishing a message)

```go

    // opening a topic looks like this
	package main 
    import (
        "context"
    
        "gocloud.dev/pubsub"
        _ "github.com/pitabwire/natspubsub"
    )
	...
	ctx := context.Background()
    // pubsub.OpenTopic creates a *pubsub.Topic from a URL.
    // This URL will Dial the NATS server at the URL in the environment variable
    // NATS_SERVER_URL and send messages with subject "example.mysubject".
    topic, err := pubsub.OpenTopic(ctx, "nats://localhost:4222?jetstream=true&stream_name=example&subject=example.mysubject")
    if err != nil {
        return err
    }
	defer topic.Shutdown(ctx)
	
    err := topic.Send(ctx, &pubsub.Message{
        Body: []byte("Hello, World!\n"),
        Metadata: map[string]string{
            "language":   "en",
            "importance": "high",
        },
    })
    if err != nil {
    return err
    }
    
    
```

# (receiving messages)

```go
package main
import (
    "context"

    "gocloud.dev/pubsub"
    _ "github.com/pitabwire/natspubsub"
)
...
ctx := context.Background()
subs, err := pubsub.OpenSubscription(ctx, "nats://localhost:4222?jetstream=true&stream_name=example&subject=example.mysubject")
if err != nil {
    return fmt.Errorf("could not open topic subscription: %v", err)
}

// Loop on received messages.
for {
    msg, err := subs.Receive(ctx)
    if err != nil {
        // Errors from Receive indicate that Receive will no longer succeed.
        log.Printf("Receiving message: %v", err)
        break
    }
    // Do work based on the message, for example:
    fmt.Printf("Got message: %q\n", msg.Body)
    // Messages must always be acknowledged with Ack.
    msg.Ack()
}

defer subs.Shutdown(ctx)


```


# URLs

For pubsub.OpenTopic and pubsub.OpenSubscription, natspubsub registers
for the scheme "nats". 

The default URL opener will connect to a default server based on the
environment variable "NATS_SERVER_URL".

This implementation supports all versions of NATS Servers, we however recommend version 2.2.0 or later.
Messages can be encoded using native NATS message headers, and native message content
providing full support for non-Go clients. Operating in this manner is  more 
efficient than using gob.Encoder. Using older versions of the server will result in 
errors and untested scenarios. 

In the event the user cannot upgrade the nats server, we recommend the use of 
the in tree implementation instead: https://github.com/google/go-cloud/tree/master/pubsub/natspubsub

To customize the URL opener, or for more details on the URL format,
see URLOpener.
See https://gocloud.dev/concepts/urls/ for background information.

Example URLs:

    Topic open - nats://127.0.0.1:4222?jetstream=true&stream_name=testq&stream_retention=workqueue&stream_storage=memory&stream_subjects=testq.*&subject=testq
   
    Topic subscription - nats://127.0.0.1:4222?consumer_ack_policy=explicit&consumer_ack_wait=10s&consumer_deliver_policy=all&consumer_durable_name=durabletest&consumer_filter_subject=testq.*&jetstream=true&stream_name=testq&stream_retention=workqueue&stream_storage=memory&stream_subjects=testq.*

This driver also has a hidden super power to make dynamic publishes from the header content supplied.
One just needs to set the header that should be checked to extend the existing topic subject during publishing:

 - **header_to_extended_subject**

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

Comprehensive definitions of the options can be found below. These are based on the official NATS JetStream configuration from:
  - [NATS JetStream Stream Configuration](https://docs.nats.io/nats-concepts/jetstream/streams)
  - [NATS JetStream Consumer Configuration](https://docs.nats.io/nats-concepts/jetstream/consumers)

## Basic Options

| Option     | Default value | Description |
|------------|---------------|-------------|
| jetstream  | true          | Enables JetStream functionality for at-least-once delivery semantics |
| subject    |               | A string of characters that form a name which the publisher and subscriber can use to find each other |

## Stream Configuration Parameters

Stream config options are prefixed by "stream_" in URL query parameters.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stream_name` | string | (required) | The unique name of the stream |
| `stream_description` | string | | Optional description of the stream |
| `stream_subjects` | []string | | List of subjects that the stream is listening on (supports wildcards) |
| `stream_retention` | string | `limits` | Message retention policy: `limits`, `interest`, or `workqueue` |
| `stream_max_consumers` | int | -1 | Maximum number of consumers allowed for the stream (-1 for unlimited) |
| `stream_max_msgs` | int64 | -1 | Maximum number of messages the stream will store (-1 for unlimited) |
| `stream_max_bytes` | int64 | -1 | Maximum total size of messages the stream will store (-1 for unlimited) |
| `stream_discard` | string | `old` | Policy for handling messages when limits are reached: `old` or `new` |
| `stream_discard_new_per_subject` | bool | false | Whether to discard new messages per subject when limits are reached |
| `stream_max_age` | duration | | Maximum age of messages that the stream will retain (e.g., "24h") |
| `stream_max_msgs_per_subject` | int64 | -1 | Maximum number of messages per subject that the stream will retain (-1 for unlimited) |
| `stream_max_msg_size` | int32 | -1 | Maximum size of any single message in the stream in bytes (-1 for unlimited) |
| `stream_storage` | string | `file` | Storage backend type: `file` or `memory` |
| `stream_num_replicas` | int | 1 | Number of replicas to maintain in clustered JetStream (1-5) |
| `stream_no_ack` | bool | false | Flag to disable acknowledging messages received by this stream |
| `stream_duplicate_window` | duration | 2m | Duration within which to track duplicate messages |
| `stream_placement` | object | | Placement constraints for stream (cluster name and/or tags) |
| `stream_mirror` | object | | Configuration for mirroring another stream |
| `stream_sources` | []object | | List of streams this stream sources messages from |
| `stream_sealed` | bool | false | Flag to mark stream as sealed (no new messages or deletion) |
| `stream_deny_delete` | bool | false | Flag to restrict ability to delete messages via API |
| `stream_deny_purge` | bool | false | Flag to restrict ability to purge messages via API |
| `stream_allow_rollup_hdrs` | bool | false | Flag to allow the Nats-Rollup header for stream content replacement |
| `stream_compression` | string | | Message storage compression algorithm: `none`, `s2`, or `gzip` |
| `stream_first_seq` | uint64 | 1 | Initial sequence number for the first message in the stream |
| `stream_subject_transform` | object | | Configuration for transforming subjects on messages |
| `stream_republish` | object | | Configuration for republishing messages after storing |
| `stream_allow_direct` | bool | false | Flag to enable direct access to individual messages |
| `stream_mirror_direct` | bool | false | Flag to enable direct access to messages from origin stream |
| `stream_consumer_limits` | object | | Limits for consumers on this stream |
| `stream_metadata` | map | | User-defined metadata key-value pairs for the stream |
| `stream_allow_msg_ttl` | bool | false | Flag to allow header-initiated per-message TTLs |
| `stream_subject_delete_marker_ttl` | duration | | Duration for keeping server markers for deletions |

## Consumer Configuration Parameters

Consumer config options are prefixed by "consumer_" in URL query parameters.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `consumer_name` | string | (auto) | Name for the consumer (auto-generated if not provided) |
| `consumer_durable_name` | string | | Name for durable subscription (persists across reconnects) |
| `consumer_description` | string | | Optional description of the consumer |
| `consumer_deliver_policy` | string | `all` | Where to start delivering messages: `all`, `last`, `new`, `by_start_sequence`, `by_start_time` |
| `consumer_opt_start_seq` | uint64 | | Starting sequence number when deliver policy is `by_start_sequence` |
| `consumer_opt_start_time` | time | | Starting time when deliver policy is `by_start_time` (RFC3339 format) |
| `consumer_ack_policy` | string | `explicit` | How messages are acknowledged: `none`, `all`, `explicit` |
| `consumer_ack_wait` | duration | 30s | How long server waits for acknowledgement before redelivery |
| `consumer_max_deliver` | int | -1 | Maximum delivery attempts for a message (-1 for unlimited) |
| `consumer_backoff` | []duration | | Back-off intervals for retrying message delivery (comma separated) |
| `consumer_filter_subject` | string | | Subject filter to select messages from stream |
| `consumer_filter_subjects` | []string | | Multiple subject filters to select messages from stream |
| `consumer_replay_policy` | string | `instant` | Rate at which messages are sent: `instant` or `original` |
| `consumer_rate_limit_bps` | uint64 | | Maximum rate of message delivery in bits per second |
| `consumer_sample_freq` | string | | Frequency for sampling acknowledgements (e.g., "100%") |
| `consumer_max_waiting` | int | 512 | Maximum number of pull requests waiting to be fulfilled |
| `consumer_max_ack_pending` | int | 1000 | Maximum number of unacknowledged messages (-1 for unlimited) |
| `consumer_headers_only` | bool | false | Flag to deliver only message headers without payload |
| `consumer_max_batch` | int | | Maximum batch size for a single pull request |
| `consumer_max_expires` | duration | | Maximum duration a pull request will wait for messages |
| `consumer_max_bytes` | int | | Maximum total bytes that can be requested in a batch |
| `consumer_inactive_threshold` | duration | 5s | Duration after which an inactive consumer is cleaned up |
| `consumer_num_replicas` | int | | Number of replicas for the consumer state (inherits from stream by default) |
| `consumer_mem_storage` | bool | false | Flag to force consumer to use memory storage |
| `consumer_metadata` | map | | User-defined metadata key-value pairs for the consumer |
| `consumer_pause_until` | time | | Timestamp until which the consumer is paused (RFC3339 format) |
| `consumer_priority_policy` | string | | Priority policy for the consumer: `standard` or `pinned` |
| `consumer_priority_timeout` | duration | | Time after which client will be unpinned if inactive |
| `consumer_priority_groups` | []string | | List of priority groups this consumer supports |

## Batch Receive Parameters

Batch receive options control how messages are batched when receiving.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `receive_batch_max_handlers` | int | 10 | Maximum number of handlers for processing batch receive operations |
| `receive_batch_min_batch_size` | int | 1 | Minimum number of messages in a batch for batch receive |
| `receive_batch_max_batch_size` | int | 100 | Maximum number of messages in a batch for batch receive |
| `receive_batch_max_batch_byte_size` | int | 1048576 | Maximum total byte size of messages in a batch (1MB default) |

## Batch Ack Parameters

Batch ack options control how message acknowledgments are batched.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ack_batch_max_handlers` | int | 10 | Maximum number of handlers for processing batch acknowledgement operations |
| `ack_batch_min_batch_size` | int | 1 | Minimum number of messages in a batch for batch acknowledgement |
| `ack_batch_max_batch_size` | int | 100 | Maximum number of messages in a batch for batch acknowledgement |
| `ack_batch_max_batch_byte_size` | int | 1048576 | Maximum total byte size of messages in a batch (1MB default) |

# Publishing options
     
| Option | Default value | Description |
|--------|---------------|-------------|
| Subject | | An identifier that publishers send messages to subscribers of interest
