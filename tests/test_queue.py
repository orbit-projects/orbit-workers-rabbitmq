from __future__ import annotations

import asyncio
import sys
import types
from typing import Any

import pytest


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

from orbit_workers_rabbitmq import (  # noqa: E402
    RabbitMQWorkQueue,
    RabbitMQWorkQueueConfig,
    RabbitMQWorkQueueError,
)


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
        self.close_attempts = 0

    async def channel(self, **kwargs: Any) -> _Channel:
        channel = _Channel()
        self.channels.append(channel)
        return channel

    async def close(self) -> None:
        self.close_attempts += 1
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


async def test_close_finishes_if_first_shutdown_caller_is_cancelled(monkeypatch: Any) -> None:
    """Repeated shutdown can await the same close operation after caller cancellation."""
    connection = await _install_connection(monkeypatch)
    queue = RabbitMQWorkQueue(_config())
    await queue.startup()
    closing = asyncio.Event()
    finish_close = asyncio.Event()

    async def delayed_close() -> None:
        closing.set()
        await finish_close.wait()
        connection.closed = True

    monkeypatch.setattr(connection, "close", delayed_close)
    first_caller = asyncio.create_task(queue.aclose())
    await closing.wait()
    first_caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_caller

    assert connection.closed is False
    finish_close.set()
    await queue.aclose()
    assert connection.closed is True


async def test_failed_connection_close_can_be_retried(monkeypatch: Any) -> None:
    connection = await _install_connection(monkeypatch)
    queue = RabbitMQWorkQueue(_config())
    await queue.startup()

    async def close_once_with_failure() -> None:
        connection.close_attempts += 1
        if connection.close_attempts == 1:
            raise RuntimeError("temporary broker close failure")
        connection.closed = True

    monkeypatch.setattr(connection, "close", close_once_with_failure)
    with pytest.raises(RabbitMQWorkQueueError, match="close"):
        await queue.aclose()
    await queue.aclose()

    assert connection.close_attempts == 2
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


async def test_plugin_control_action_reviews_safe_queue_settings() -> None:
    """The UI can review queue limits without connecting or revealing the broker URL."""
    from orbit_workers_rabbitmq.plugin import RabbitMQWorkQueuePlugin

    secret_url = "amqps://operator:private-secret@broker.example/vhost"
    queue = RabbitMQWorkQueue(RabbitMQWorkQueueConfig(url=secret_url, queue="orders.work"))
    plugin = RabbitMQWorkQueuePlugin(queue)

    result = await plugin.control_actions[0].handler({})

    assert result is not None
    assert result.summary == "RabbitMQ worker queue settings are configured."
    assert "queue: orders.work" in result.details
    assert all("private-secret" not in detail for detail in result.details)
