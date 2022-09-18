# Multicore Aware AMQP client
AMQP client based on EIO, to allow taking advantage of effect based
ocaml.

[![Main workflow](https://github.com/andersfugmann/amqp-client-eio/actions/workflows/workflow.yml/badge.svg)](https://github.com/andersfugmann/amqp-client-eio/actions/workflows/workflow.yml)
## Design

### Goals

#### No need for explicit locking.
Data will be communicated over streams / channels or promises.

#### Data processing should be done as close to user a possible
When sending data, data processing (frame encapsulation) should be
done by the calling thread.





### Connection
Establish and setup a connection to the amqp server. This just holds
simple primitives.
Each connection will have a sender and a receiver thread. These will
be locked to the same domain (But does not have to)

The sender will send everything posted on the sender channel
(potentially

The receiver will relay data from the clannels

When creating a connection - should we use the control flow?
(Thats a design choice that can be changed later!)



### Channel
A channel has a function to send, and a receiver channel.
Data is relayed from the connection on the receiver channel, which
will lookup receiver to post data to.

### Consuming from a queue
Setting up consumption will return a queue for client consumption

### Error handling
Error handling is done using exceptions
Connection errors are relayed to all channels.

Channel errors are relayed to all waiters.
When sending in ack mode and
