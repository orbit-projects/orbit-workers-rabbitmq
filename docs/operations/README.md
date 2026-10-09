# Operations

Configure `RabbitMQWorkQueueConfig` with an `amqps://` endpoint for production and a secret manager
or workload identity for credential delivery. Restrict the account to the required virtual host,
exchange, and queue operations. Do not log the URL, payloads, or provider secrets.

The adapter declares a quorum work queue, durable direct dead-letter exchange, and quorum
`<queue>.dead` queue. Configure RabbitMQ persistence, quorum membership, disk alarms, resource
limits, backups, TLS certificate rotation, and monitoring. The dead-letter queue can grow
indefinitely; alert on depth and document retention, inspection, and replay procedures.

Publication returns only after publisher confirmation and mandatory routing succeed. A timeout or
connection loss can make the outcome ambiguous; return a retryable webhook response and tolerate
duplicate provider deliveries. The consumer uses manual acknowledgments. Handler success precedes
ack; retry republishes a persistent message with an incremented attempt before acking the prior
delivery. Terminal rejects route to the dead-letter queue. Delivery is at least once.

The source quorum queue configures at-least-once dead-letter transfer and `reject-publish` overflow.
This requires sufficient broker storage and a healthy dead-letter target; monitor both queues and
their nodes so backpressure is visible. Three quorum members across independent nodes is the
practical minimum for tolerating one node failure. Configure `quorum_initial_group_size` to match
the actual cluster. A one-member queue is for local development only and provides no node
fault-tolerance. RabbitMQ 3.10 introduced at-least-once dead lettering; verify the `stream_queue`
feature flag is enabled on the cluster. All publishers and consumers must use the same queue,
dead-letter, group-size, and capacity settings or queue declaration can fail with an argument
mismatch.

Webhook ingress should verify the exact raw body before publishing. Return `2xx` only after
confirmed publication; otherwise return a retryable failure. Consumer code must enforce a unique
provider/account/event key in the same database transaction as order/payment state changes. Do not
send email or trigger fulfillment outside a transaction/outbox or other idempotent mechanism.

Validate broker persistence and failover with the exact production topology. Unit tests do not
establish that a live RabbitMQ cluster meets durability or availability requirements. See the
official [quorum queue guide](https://www.rabbitmq.com/docs/quorum-queues) and [publisher confirms
guide](https://www.rabbitmq.com/docs/confirms) for operator-level semantics.
