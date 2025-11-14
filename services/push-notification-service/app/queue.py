import json
import logging
import uuid
from typing import Any, Dict, Optional

import aio_pika


logger = logging.getLogger(__name__)


class MessageQueueManager:
    """Wrapper around RabbitMQ for publishing/consuming notification messages."""

    def __init__(self, rabbitmq_url: str, prefetch_count: int = 10):
        self.rabbitmq_url = rabbitmq_url
        self.prefetch_count = prefetch_count
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.exchange_name = "notifications.direct"
        self.exchange: Optional[aio_pika.Exchange] = None
        self.push_queue = "push.queue"
        self.retry_queue = "push.queue.retry"
        self.dead_letter_queue = "failed.queue"

    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=self.prefetch_count)

            self.exchange = await self.channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.DIRECT,
                durable=True
            )

            push_queue_obj = await self.channel.declare_queue(
                self.push_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.exchange_name,
                    "x-dead-letter-routing-key": self.dead_letter_queue
                }
            )

            retry_queue_obj = await self.channel.declare_queue(
                self.retry_queue,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.exchange_name,
                    "x-dead-letter-routing-key": self.dead_letter_queue
                }
            )

            failed_queue_obj = await self.channel.declare_queue(
                self.dead_letter_queue,
                durable=True
            )

            await push_queue_obj.bind(self.exchange, routing_key=self.push_queue)
            await retry_queue_obj.bind(self.exchange, routing_key=self.retry_queue)
            await failed_queue_obj.bind(self.exchange, routing_key=self.dead_letter_queue)

            logger.info("Connected to RabbitMQ with exchange-based routing successfully")
        except Exception as exc:
            logger.error("Failed to connect to RabbitMQ: %s", exc)
            raise

    async def publish_message(self, routing_key: str, message: Dict[str, Any]):
        if not self.channel or not self.exchange:
            await self.connect()

        await self.exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=message.get("idempotency_key") or str(uuid.uuid4())
            ),
            routing_key=routing_key
        )

    async def close(self):
        if self.connection:
            await self.connection.close()

