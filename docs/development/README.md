# Development

The package supports Python 3.11–3.14. Install the sibling contracts from a workspace checkout:

```bash
python -m pip install -e ../orbit-core -e ../orbit-workers
python -m pip install -e '.[dev]'
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Use deterministic fake AMQP surfaces for unit tests. A separately opted-in integration job should
exercise a disposable RabbitMQ service, including confirms, restart/redelivery, dead-letter routing,
retry ambiguity, and concurrent shutdown. Never point destructive tests at a shared or production
broker.
