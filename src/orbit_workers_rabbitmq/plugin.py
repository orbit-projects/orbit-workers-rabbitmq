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
"""Optional Core lifecycle integration for the standalone RabbitMQ work adapter."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata
from orbit_workers import WORK_QUEUE_DEPENDENCY_KEY

from orbit_workers_rabbitmq.queue import RabbitMQWorkQueue

WORK_PUBLISHER_DEPENDENCY_KEY = "orbit.work_publisher"


class RabbitMQWorkQueuePlugin(Plugin):
    """Register publisher/consumer contracts and own RabbitMQ adapter shutdown."""

    metadata = PluginMetadata(
        name="orbit-workers-rabbitmq",
        version="0.1.0a1",
        capabilities=frozenset({"workers.rabbitmq"}),
    )

    def __init__(self, queue: RabbitMQWorkQueue) -> None:
        if not isinstance(queue, RabbitMQWorkQueue):
            raise TypeError("queue must be a RabbitMQWorkQueue instance.")
        self.queue = queue

    def setup(self, application: Application) -> None:
        if not isinstance(application, Application):
            raise TypeError("RabbitMQWorkQueuePlugin requires an Orbit Application.")
        application.container.register_instance(WORK_QUEUE_DEPENDENCY_KEY, self.queue)
        application.container.register_instance(WORK_PUBLISHER_DEPENDENCY_KEY, self.queue)

    async def activate(self) -> None:
        """Verify broker availability and provision topology during application startup."""
        await self.queue.startup()

    async def deactivate(self) -> None:
        await self.queue.aclose()


__all__ = ["RabbitMQWorkQueuePlugin", "WORK_PUBLISHER_DEPENDENCY_KEY"]
