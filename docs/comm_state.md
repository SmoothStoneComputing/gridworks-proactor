# Communication state

The `Proactor` maintains communication state ("active" or not active) for each external point-to-point communications
link. The "active" state is intended to indicate that not only is the underlying communications channel (e.g. MQTT)
healthy, but also that a valid application-level message has been recently received from the peer at the other end of
the communications link. This state information is intended to allow the application derived from the Proactor
to determine if it must make local decisions while the peer is disconnected, non-responsive, slow or otherwise impaired.
Additionally, visibility into the history of communication is provided to (human) monitors of the system through Events
generated at each transition of the the comm state.

## "active" communication state definition

A communication link is "active" if _all_ of these are true:

1. The underlying communications mechanism (MQTT) is connected.
2. All input channels of underlying mechanism (MQTT topics) are established.
3. All application messages requiring acknowledgement have been ACKed in timely fashion (by default 5 seconds).
4. A valid message has been received "recently" (by default within 1 minute) from the peer application.

Note after the underying communication mechanism reports a connection, before communication can be considered "active",
requirements 2 and 4 above must be met. That is, all input channels must be established and at least one valid
application message must be received from the peer. Requirement 2 is present because otherwise we could send a message
but not hear the response to it from the peer. Requirement 4 is present because we could have good underlying
communication (e.g. a connection to an MQTT broker), without the peer application actually running. Requirement 3 is
not applied until after the "active" state has been reached.

This diagram approximates how the "active" state is achieved, maintained and lost:

```{mermaid}
flowchart TB
linkStyle default interpolate basis
NonExistent(Non existent) -- constructed --> not_started
subgraph NotActive[not active &nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp]
not_started -- start_called --> connecting

connecting -- mqtt_connect_failed --> connecting
connecting -- mqtt_connected --> awaiting_setup_and_peer

awaiting_setup_and_peer -- mqtt_suback --> awaiting_setup_and_peer
awaiting_setup_and_peer -- mqtt_suback --> optimism
awaiting_setup_and_peer -- message_from_peer --> awaiting_setup
awaiting_setup_and_peer -- mqtt_disconnected  --> connecting

awaiting_setup -- mqtt_suback --> awaiting_setup
awaiting_setup -- mqtt_disconnected  --> connecting

optimism -- mqtt_disconnected --> connecting
optimism -- response_timeout --> awaiting_peer

awaiting_peer -- mqtt_disconnected  --> connecting
end

awaiting_setup -- mqtt_suback --> active

optimism -- message_from_peer --> active
awaiting_peer -- message_from_peer --> active

active -- response_timeout --> awaiting_peer
active -- mqtt_disconnected --> connecting
```

Much of the complexity in this diagram results from asynchronously accumulating input channel establishments and a
message from the peer upon restore of the underlying connection. After restoring communication to the underlying
communication mechanism (e.g. an MQTT broker), we must get acknowledgements of all our subscriptions and a message from
the peer before the link is considered "active". There could be more than one subscription acknowledgement message, and
these and the message from the peer could arrive in any order. This complexity could be reduced by serializing the
accumulation of these results, at the cost of longer time to re-activate after restore of the underlying communication
mechanism.

## LinkManager

The [](gwproactor.links) package implements most of the Proactor's communication infrastructure. [](LinkManager) is the
interface to this package used by [](Proactor). The interaction between them can be seen by searching the code for
`_links`. This search should produce approximately the following entry points in the message processing loop:

1. Start on user request.
2. Stop on user request (omitted from the diagram for clarity).
3. Handle connect of underlying comm mechanism (e.g. broker connect of MQTT).
4. Handle disconnect of underlying comm mechanism.
5. Handle intermediate connection establishment events from underlying comm mechanism (e.g. subscription ack of MQTT).
6. Send acks for incoming messages that require ack.
7. Receive acks for outgoing messages that require acks.
8. Handle timeouts for ack receipt.
9. Update “heard from recently” on message receipt.
10. Handle timeouts for “heard from recently”.

### LinkManager helpers

The [](LinkManager) uses these helpers:

- [](MQTTClients), to manage Paho MQTT clients.
- a dict of [MQTTCodec](https://github.com/thegridelectric/gridworks-protocol/blob/dev/src/gwproto/decoders.py),
  to contain a message coder/decoder for each MQTT client.
- [](LinkStates), to manage the communications state machine for each link.
- [](MessageTimes), to track the times of last send and receive for each link.
- [](TimerManagerInterface), to start and cancel timers for acknowledgement timeout.
- [](AckManager), to start, track, handle and cancel timers for pending acknowledgements.
- [](PersisterInterface), to persist unacknowledged Events on loss of "active" communication and re-upload them when
  "active" state is restored.
- [](ProactorLogger), to log communications state tranisitions.
- [](ProactorStats), to update various statistics about communications.
