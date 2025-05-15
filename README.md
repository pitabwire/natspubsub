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

Stream config options are prefixed by "stream_" while consumer config options are prefixed by "consumer_"

see : https://github.com/nats-io/nats.go/blob/v1.42.0/jetstream/stream_config.go#L58 for config names in the json part
also : https://github.com/nats-io/nats.go/blob/v1.42.0/jetstream/consumer_config.go#L102 


# Publishing options
     
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Option | Default value |                Description 
|--------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
| Subject       |               | An identifier that publishers send messages to subscribers of interest
