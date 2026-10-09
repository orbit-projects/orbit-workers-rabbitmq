# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Confirmed RabbitMQ publication and manually acknowledged work deliveries."""

from __future__ import annotations

import asyncio
import logging
import math
from contextlib import suppress
from typing import Any

import aio_pika
from orbit_workers import WorkDelivery, WorkMessage, WorkPublisher, WorkQueue

from orbit_workers_rabbitmq.config import RabbitMQWorkQueueConfig

_LOG = logging.getLogger("orbit.workers.rabbitmq")
_ATTEMPT_HEADER = "x-orbit-attempt"
_MAX_ATTEMPTS = 1_000_000_000


class RabbitMQWorkQueueError(RuntimeError):
    """Sanitized RabbitMQ failure without connection, payload, or credential details."""


class RabbitMQWorkQueue(WorkQueue, WorkPublisher):
    """Persistent RabbitMQ queue implementing Orbit's producer and worker contracts.

    Successful ``publish`` awaits publisher confirmation. Queue declarations are durable, messages
    are persistent, consumers use bounded prefetch/manual acknowledgements, and rejected terminal
    deliveries route to an adapter-declared durable dead-letter queue. Delivery is at least once.
    """

    def __init__(self, config: RabbitMQWorkQueueConfig) -> None:
        """Bind validated settings; connections and channels are created lazily."""
        if not isinstance(config, RabbitMQWorkQueueConfig):
            raise TypeError("config must be a RabbitMQWorkQueueConfig.")
        self.config = config
        self._connection: Any | None = None
        self._publisher: Any | None = None
        self._consumer_channel: Any | None = None
        self._queue: Any | None = None
        self._consumer_tag: str | None = None
        self._incoming: asyncio.Queue[RabbitMQWorkDelivery] = asyncio.Queue(
            maxsize=config.prefetch_count
        )
        self._lock = asyncio.Lock()
        self._publish_slots = asyncio.Semaphore(config.max_in_flight_publishes)
        self._closed = False

    async def publish(self, message: WorkMessage) -> None:
        """Publish one persistent message and wait until RabbitMQ confirms it."""
        if not isinstance(message, WorkMessage):
            raise TypeError("publish requires an Orbit WorkMessage.")
        if message.queue != self.config.queue:
            raise ValueError("WorkMessage queue does not match this RabbitMQ adapter.")
        if len(message.payload) > self.config.max_message_bytes:
            raise ValueError("WorkMessage payload exceeds the configured RabbitMQ limit.")
        channel = await self._ensure_publisher()
        broker_message = aio_pika.Message(
            body=message.payload,
            content_type="application/octet-stream",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers={**message.headers, _ATTEMPT_HEADER: 1},
        )
        try:
            async with asyncio.timeout(self.config.publish_timeout_seconds):
                async with self._publish_slots:
                    if self._closed:
                        raise RabbitMQWorkQueueError("publish")
                    await channel.default_exchange.publish(
                        broker_message,
                        routing_key=self.config.queue,
                        mandatory=True,
                        timeout=self.config.publish_timeout_seconds,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RabbitMQWorkQueueError("publish") from None

    async def receive(self, *, timeout: float) -> WorkDelivery | None:
        """Wait for one manually acknowledged delivery, or return ``None`` at timeout."""
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not (math.isfinite(timeout) and 0 < timeout <= 60)
        ):
            raise ValueError("timeout must be finite and between zero and 60 seconds.")
        await self._ensure_consumer()
        try:
            return await asyncio.wait_for(self._incoming.get(), timeout=float(timeout))
        except TimeoutError:
            return None

    async def startup(self) -> None:
        """Establish the broker connection and declare topology before a host reports ready."""
        await self._ensure_publisher()

    async def aclose(self) -> None:
        """Stop broker delivery and close all owned channels and the robust connection."""
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            connection = self._connection
            self._connection = None
            self._publisher = None
            self._consumer_channel = None
            self._queue = None
            self._consumer_tag = None
        if connection is not None:
            try:
                await connection.close()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQWorkQueueError("close") from None

    async def __aenter__(self) -> RabbitMQWorkQueue:
        """Return this queue for async context-managed resource ownership."""
        if self._closed:
            raise RabbitMQWorkQueueError("connect")
        await self.startup()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close adapter-owned AMQP resources on context exit."""
        await self.aclose()

    async def _ensure_publisher(self) -> Any:
        async with self._lock:
            if self._closed:
                raise RabbitMQWorkQueueError("publish")
            if self._publisher is None:
                await self._connect_locked()
                assert self._connection is not None
                try:
                    channel = await self._connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    await self._declare_topology(channel)
                    self._publisher = channel
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise RabbitMQWorkQueueError("publisher setup") from None
            return self._publisher

    async def _ensure_consumer(self) -> None:
        async with self._lock:
            if self._closed:
                raise RabbitMQWorkQueueError("receive")
            if self._consumer_channel is not None:
                return
            await self._connect_locked()
            assert self._connection is not None
            try:
                channel = await self._connection.channel()
                await channel.set_qos(prefetch_count=self.config.prefetch_count)
                queue = await self._declare_topology(channel)
                self._consumer_channel = channel
                self._queue = queue
                self._consumer_tag = await queue.consume(self._on_message, no_ack=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQWorkQueueError("consumer setup") from None

    async def _connect_locked(self) -> None:
        if self._connection is None:
            try:
                self._connection = await aio_pika.connect_robust(
                    self.config._connection_url(), timeout=self.config.connect_timeout_seconds
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQWorkQueueError("connect") from None

    async def _declare_topology(self, channel: Any) -> Any:
        """Declare durable work and dead-letter paths before publishing or consuming."""
        try:
            dead_exchange = await channel.declare_exchange(
                self.config.dead_letter_exchange,
                aio_pika.ExchangeType.DIRECT,
                durable=True,
            )
            dead_queue_name = f"{self.config.queue}.dead"
            dead_queue = await channel.declare_queue(
                dead_queue_name,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-quorum-initial-group-size": self.config.quorum_initial_group_size,
                    "x-max-length-bytes": self.config.max_dead_letter_bytes,
                    "x-overflow": "reject-publish",
                },
            )
            await dead_queue.bind(dead_exchange, routing_key=self.config.queue)
            return await channel.declare_queue(
                self.config.queue,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-quorum-initial-group-size": self.config.quorum_initial_group_size,
                    "x-max-length-bytes": self.config.max_queue_bytes,
                    "x-delivery-limit": self.config.max_broker_redeliveries,
                    "x-dead-letter-exchange": self.config.dead_letter_exchange,
                    "x-dead-letter-routing-key": self.config.queue,
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RabbitMQWorkQueueError("topology declaration") from None

    async def _on_message(self, incoming: Any) -> None:
        """Validate broker metadata before exposing a bounded work delivery."""
        try:
            body = incoming.body
            message_id = incoming.message_id
            headers = incoming.headers or {}
            attempt = headers.get(_ATTEMPT_HEADER, 1)
            if (
                not isinstance(body, bytes)
                or len(body) > self.config.max_message_bytes
                or not isinstance(message_id, str)
                or not isinstance(attempt, int)
                or isinstance(attempt, bool)
                or not 1 <= attempt <= _MAX_ATTEMPTS
            ):
                raise ValueError
            public_headers = {
                str(key): str(value)
                for key, value in headers.items()
                if isinstance(key, str)
                and key != _ATTEMPT_HEADER
                and isinstance(value, (str, int, float, bool))
            }
            work_message = WorkMessage(
                queue=self.config.queue,
                message_id=message_id,
                payload=body,
                headers=public_headers,
            )
            delivery = RabbitMQWorkDelivery(self, incoming, work_message, attempt)
            self._incoming.put_nowait(delivery)
        except asyncio.CancelledError:
            raise
        except asyncio.QueueFull:
            with suppress(Exception):
                await incoming.reject(requeue=True)
            _LOG.error("RabbitMQ delivery buffer exceeded configured prefetch.")
        except Exception:
            with suppress(Exception):
                await incoming.reject(requeue=False)
            _LOG.warning("Rejected malformed RabbitMQ work delivery.")

    async def _retry(self, delivery: RabbitMQWorkDelivery, delay: float) -> None:
        """Republish with an incremented attempt, confirm, then acknowledge the old delivery."""
        if (
            isinstance(delay, bool)
            or not isinstance(delay, (int, float))
            or not (math.isfinite(delay) and 0 <= delay <= 86_400)
        ):
            raise ValueError("retry delay is invalid.")
        await asyncio.sleep(float(delay))
        channel = await self._ensure_publisher()
        headers: dict[str, Any] = dict(delivery.message.headers)
        headers[_ATTEMPT_HEADER] = delivery.attempt + 1
        broker_message = aio_pika.Message(
            body=delivery.message.payload,
            content_type="application/octet-stream",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=delivery.message.message_id,
            headers=headers,
        )
        async with self._publish_slots:
            try:
                async with asyncio.timeout(self.config.publish_timeout_seconds):
                    await channel.default_exchange.publish(
                        broker_message,
                        routing_key=self.config.queue,
                        mandatory=True,
                        timeout=self.config.publish_timeout_seconds,
                    )
                    await delivery._incoming.ack(multiple=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQWorkQueueError("retry") from None


class RabbitMQWorkDelivery(WorkDelivery):
    """One RabbitMQ delivery with exactly-once local settlement and at-least-once semantics."""

    def __init__(
        self,
        owner: RabbitMQWorkQueue,
        incoming: Any,
        message: WorkMessage,
        attempt: int,
    ) -> None:
        self._owner = owner
        self._incoming = incoming
        self._message = message
        self._attempt = attempt
        self._settled = False
        self._settlement_lock = asyncio.Lock()

    @property
    def message(self) -> WorkMessage:
        return self._message

    @property
    def attempt(self) -> int:
        return self._attempt

    async def ack(self) -> None:
        await self._settle("ack")

    async def reject(self) -> None:
        await self._settle("reject")

    async def retry(self, delay: float) -> None:
        async with self._settlement_lock:
            if self._settled:
                raise RabbitMQWorkQueueError("settle")
            await self._owner._retry(self, delay)
            self._settled = True

    async def _settle(self, action: str) -> None:
        async with self._settlement_lock:
            if self._settled:
                raise RabbitMQWorkQueueError("settle")
            try:
                if action == "ack":
                    await self._incoming.ack(multiple=False)
                else:
                    await self._incoming.reject(requeue=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise RabbitMQWorkQueueError("settle") from None
            self._settled = True


__all__ = ["RabbitMQWorkDelivery", "RabbitMQWorkQueue", "RabbitMQWorkQueueError"]
