# Security

- Keep broker URLs in a secret manager; Pydantic redacts them in representations.
- Prefer AMQPS, trust only the configured broker certificates, and use least-privilege vhost
  permissions.
- Treat work payloads and dead-letter messages as sensitive. They may contain verified payment
  webhook bodies; encrypt broker disks and restrict inspection/replay access.
- Do not log message bodies, webhook signatures, secrets, or payment data.
- Set message size and prefetch limits deliberately; this adapter caps messages at 1 MiB and
  prefetch at the configured bounded value.
- Consumers must validate envelopes again and be idempotent. Broker access is not proof that a
  message is semantically safe.
- Rotate credentials and TLS certificates using the deployment's supported broker process.
