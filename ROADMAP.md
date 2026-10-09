# Roadmap

This roadmap records current direction, not release promises.

## Implemented

- Persistent RabbitMQ publication with mandatory routing and publisher confirmation.
- Durable queue and dead-letter topology, bounded prefetch, and explicit acknowledgements.
- Confirmed retry republishing with attempt metadata.
- Optional Orbit Core lifecycle registration for the queue and publisher contracts.

## Follow-up

- Live RabbitMQ integration tests and tested cluster failover profiles.
- Metrics and health integration after their provider-neutral contracts are selected.
- Operator-managed policies for queue limits, retention, and dead-letter replay.
