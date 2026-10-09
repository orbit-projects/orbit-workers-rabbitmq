# Orbit Workers RabbitMQ

`orbit-workers-rabbitmq` implements the `orbit-workers` publisher and consumer contracts with a
durable RabbitMQ queue. Install it separately from the provider-neutral worker runner:

```bash
python -m pip install orbit-workers orbit-workers-rabbitmq
```

For a Core-hosted application, install the optional lifecycle integration with
`python -m pip install 'orbit-workers-rabbitmq[core]'` and import
`RabbitMQWorkQueuePlugin` from `orbit_workers_rabbitmq.plugin`. Standalone workers do not need Core.

It declares quorum work and dead-letter queues, publishes persistent messages with mandatory
routing and publisher confirmations, and consumes with bounded prefetch and manual acknowledgments.
Failed work is republished with an incremented attempt count and confirmed before the previous
delivery is acknowledged. Terminal rejects and malformed deliveries go to the declared dead-letter
queue. The default initial quorum group size is three; configure it to fit the broker cluster.
Bounded queue byte limits reject new publications when full so webhook handlers can return a
retryable failure. Delivery remains **at least once**: process crashes and ambiguous acknowledgments can cause
duplicates, so handlers must be idempotent.

```python
from orbit_workers import QueueWorker, RetryPolicy, WorkMessage
from orbit_workers_rabbitmq import RabbitMQWorkQueue, RabbitMQWorkQueueConfig

queue = RabbitMQWorkQueue(
    RabbitMQWorkQueueConfig(
        url="amqps://orbit-user:secret@rabbitmq.example/vhost",
        queue="payments.webhooks",
        dead_letter_exchange="payments.dead",
    )
)


async def process(message: WorkMessage) -> None:
    # Decode the verified WebhookEnvelope and persist event identity and domain state
    # idempotently in the application's database transaction.
    ...


await QueueWorker(queue, process, workers=8, retry_policy=RetryPolicy(max_attempts=8)).run()
```

The application that accepts provider webhooks can publish with this same adapter. A `2xx` response
is safe only after `publish()` returns, which means RabbitMQ confirmed the persistent message. If
publication fails or is ambiguous, return a retryable HTTP failure; provider retries can produce a
duplicate, which the consumer must deduplicate transactionally.

The adapter owns AMQP connections and queue topology, not the broker deployment. Operators still
need TLS, least-privilege credentials, replicated/persistent broker storage, backups, alerts, and
tested failover. The dead-letter queue is durable but requires an operational retention and replay
policy. See [architecture](docs/architecture/overview.md), [operations](docs/operations/README.md),
[security](docs/security/overview.md), and [development](docs/development/README.md).

This package is pre-alpha. It supports Python 3.11–3.14 and is licensed under Apache-2.0.
