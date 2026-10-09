from __future__ import annotations

import sys
import types
from typing import Any


class _Message:
    def __init__(self, body: bytes, **kwargs: Any) -> None:
        self.body = body
        self.headers = kwargs.get("headers", {})
        self.message_id = kwargs.get("message_id")
        self.content_type = kwargs.get("content_type")
        self.delivery_mode = kwargs.get("delivery_mode")


_aio_pika = types.ModuleType("aio_pika")
_aio_pika.Message = _Message  # type: ignore[attr-defined]
_aio_pika.DeliveryMode = types.SimpleNamespace(PERSISTENT=2)  # type: ignore[attr-defined]
_aio_pika.ExchangeType = types.SimpleNamespace(DIRECT="direct")  # type: ignore[attr-defined]
sys.modules["aio_pika"] = _aio_pika

from orbit_workers import WorkMessage  # noqa: E402

from orbit_workers_rabbitmq import RabbitMQWorkQueue, RabbitMQWorkQueueConfig  # noqa: E402


class _Exchange:
    def __init__(self) -> None:
        self.published: list[tuple[_Message, str, bool]] = []

    async def publish(
        self,
        message: _Message,
        *,
        routing_key: str,
        mandatory: bool = True,
        timeout: float | None = None,
    ) -> str:
        self.published.append((message, routing_key, mandatory))
        return "confirmed"


class _Queue:
    def __init__(self) -> None:
        self.callback: Any | None = None
        self.bound: list[tuple[Any, str]] = []

    async def bind(self, exchange: Any, *, routing_key: str) -> None:
        self.bound.append((exchange, routing_key))

    async def consume(self, callback: Any, *, no_ack: bool = False) -> str:
        assert no_ack is False
        self.callback = callback
        return "consumer-tag"


class _Channel:
    def __init__(self) -> None:
        self.default_exchange = _Exchange()
        self.queue = _Queue()
        self.qos: int | None = None
        self.declarations: list[tuple[str, bool, dict[str, Any] | None]] = []

    async def declare_exchange(self, name: str, kind: Any, *, durable: bool) -> _Exchange:
        self.declarations.append((name, durable, None))
        return self.default_exchange

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> _Queue:
        self.declarations.append((name, durable, arguments))
        return self.queue

    async def set_qos(self, *, prefetch_count: int) -> None:
        self.qos = prefetch_count


class _Connection:
    def __init__(self) -> None:
        self.channels: list[_Channel] = []
        self.closed = False

    async def channel(self, **kwargs: Any) -> _Channel:
        channel = _Channel()
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        self.closed = True


def _config() -> RabbitMQWorkQueueConfig:
    return RabbitMQWorkQueueConfig(url="amqps://worker:secret@broker/vhost")


async def _install_connection(monkeypatch: Any) -> _Connection:
    connection = _Connection()

    async def connect(url: str, *, timeout: float) -> _Connection:
        assert url == "amqps://worker:secret@broker/vhost"
        assert timeout == 5
        return connection

    monkeypatch.setattr(_aio_pika, "connect_robust", connect, raising=False)
    return connection


async def test_publish_uses_persistent_message_and_waits_for_mandatory_confirm(
    monkeypatch: Any,
) -> None:
    connection = await _install_connection(monkeypatch)
    queue = RabbitMQWorkQueue(_config())
    await queue.publish(WorkMessage(queue="orbit.work", message_id="event-1", payload=b"body"))

    publisher = connection.channels[0]
    message, route, mandatory = publisher.default_exchange.published[0]
    assert message.delivery_mode == 2
    assert route == "orbit.work"
    assert mandatory is True
    assert publisher.declarations[0][1] is True
    await queue.aclose()
    assert connection.closed is True


async def test_receive_uses_manual_ack_and_retries_confirm_before_ack(monkeypatch: Any) -> None:
    connection = await _install_connection(monkeypatch)
    queue = RabbitMQWorkQueue(_config())
    assert await queue.receive(timeout=0.01) is None
    consumer_channel = connection.channels[0]
    assert consumer_channel.qos == 16
    assert consumer_channel.queue.callback is not None

    incoming = _Incoming(body=b"payment", message_id="event-1", headers={})
    await consumer_channel.queue.callback(incoming)
    delivery = await queue.receive(timeout=0.1)
    assert delivery is not None
    assert delivery.attempt == 1
    await delivery.retry(0)

    assert incoming.acked == 1
    publisher_channel = connection.channels[1]
    retried, route, mandatory = publisher_channel.default_exchange.published[0]
    assert retried.headers["x-orbit-attempt"] == 2
    assert route == "orbit.work"
    assert mandatory is True
    await queue.aclose()


async def test_terminal_reject_routes_to_dead_letter_policy(monkeypatch: Any) -> None:
    connection = await _install_connection(monkeypatch)
    queue = RabbitMQWorkQueue(_config())
    assert await queue.receive(timeout=0.01) is None
    channel = connection.channels[0]
    incoming = _Incoming(body=b"payment", message_id="event-1", headers={})
    await channel.queue.callback(incoming)
    delivery = await queue.receive(timeout=0.1)
    assert delivery is not None
    await delivery.reject()
    assert incoming.rejected is True
    assert incoming.requeue is False
    dead_queue = next(item for item in channel.declarations if item[0] == "orbit.work")
    assert dead_queue[2] is not None
    assert dead_queue[2]["x-queue-type"] == "quorum"
    assert dead_queue[2]["x-dead-letter-exchange"] == "orbit.work.dlx"
    assert dead_queue[2]["x-dead-letter-routing-key"] == "orbit.work"
    assert dead_queue[2]["x-dead-letter-strategy"] == "at-least-once"
    assert dead_queue[2]["x-overflow"] == "reject-publish"
    await queue.aclose()


class _Incoming:
    def __init__(self, *, body: bytes, message_id: str, headers: dict[str, Any]) -> None:
        self.body = body
        self.message_id = message_id
        self.headers = headers
        self.acked = 0
        self.rejected = False
        self.requeue: bool | None = None

    async def ack(self, *, multiple: bool = False) -> None:
        assert multiple is False
        self.acked += 1

    async def reject(self, *, requeue: bool = False) -> None:
        self.rejected = True
        self.requeue = requeue
