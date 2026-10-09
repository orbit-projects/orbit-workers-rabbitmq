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
"""Validated and redacted RabbitMQ work queue settings."""

from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,99}\Z")


class RabbitMQWorkQueueConfig(BaseModel):
    """One durable queue topology, credentials, and bounded delivery settings."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)

    url: SecretStr
    queue: str = "orbit.work"
    dead_letter_exchange: str = "orbit.work.dlx"
    quorum_initial_group_size: int = Field(default=3, ge=1, le=9)
    prefetch_count: int = Field(default=16, ge=1, le=10_000)
    max_in_flight_publishes: int = Field(default=128, ge=1, le=10_000)
    connect_timeout_seconds: float = Field(default=5, gt=0, le=120)
    publish_timeout_seconds: float = Field(default=10, gt=0, le=120)
    max_message_bytes: int = Field(default=1_048_576, ge=1, le=1_048_576)
    max_queue_bytes: int = Field(default=1_073_741_824, ge=1_048_576, le=2**40)
    max_dead_letter_bytes: int = Field(default=268_435_456, ge=1_048_576, le=2**40)
    max_broker_redeliveries: int = Field(default=20, ge=1, le=1_000_000)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: SecretStr) -> SecretStr:
        url = value.get_secret_value()
        if (
            not 1 <= len(url) <= 4_096
            or not url.startswith(("amqp://", "amqps://"))
            or any(not character.isprintable() or character.isspace() for character in url)
        ):
            raise ValueError("RabbitMQ URL must be a bounded amqp:// or amqps:// URL.")
        return value

    @field_validator("queue", "dead_letter_exchange")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if _NAME.fullmatch(value) is None:
            raise ValueError("RabbitMQ names must use portable ASCII identifier characters.")
        return value

    def _connection_url(self) -> str:
        """Reveal credentials only at the aio-pika connection boundary."""
        return self.url.get_secret_value()


__all__ = ["RabbitMQWorkQueueConfig"]
