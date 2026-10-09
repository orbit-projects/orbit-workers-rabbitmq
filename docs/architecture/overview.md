# Architecture

`orbit-workers` owns queue-neutral `WorkQueue`, `WorkPublisher`, and bounded `QueueWorker`
contracts. This package owns RabbitMQ credentials, connections, durable topology, message
serialization, publisher confirms, delivery acknowledgments, retry republishing, and dead-letter
routing. It does not import payment-provider SDKs or own business schemas.

Work messages are persistent and routed to a named quorum queue. A confirmed retry is published
before the failed delivery is acknowledged; a crash between those operations can duplicate a
message, but does not intentionally discard it. Consumers acknowledge only after the handler
returns successfully. Rejects route to a durable dead-letter queue. This is at-least-once delivery,
not exactly-once processing. Configure initial quorum group size to match the available RabbitMQ
cluster; three members is the practical minimum for tolerating one node failure.

The adapter's queue is for work delivery. `orbit-events-rabbitmq` is a separate event transport and
does not implement the `WorkQueue` contract. Core itself does not include either transport.
