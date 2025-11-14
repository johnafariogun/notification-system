import asyncio
import json
import logging
import uuid
from typing import Optional

import aio_pika

from app.cache import CacheManager
from app.database import DatabaseManager
from app.queue import MessageQueueManager
from app.schemas import PushNotificationPayload
from app.push import PushServiceManager


logger = logging.getLogger(__name__)


class NotificationConsumer:
    """Consumes messages from RabbitMQ and sends notifications via FCM."""

    def __init__(
        self,
        queue_manager: MessageQueueManager,
        cache_manager: CacheManager,
        push_service: PushServiceManager,
        db_manager: DatabaseManager,
        max_retries: int = 3,
    ):
        self.queue_manager = queue_manager
        self.cache_manager = cache_manager
        self.push_service = push_service
        self.db_manager = db_manager
        self.max_retries = max_retries

    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            correlation_id = message.correlation_id or str(uuid.uuid4())
            notification_id: Optional[str] = None
            data = {}

            try:
                data = json.loads(message.body.decode())
                logger.info("[%s] Processing notification: %s", correlation_id, data.get("title"))

                notification_id = data.get("notification_id")
                payload = PushNotificationPayload(**data)

                if payload.idempotency_key and await self.cache_manager.check_idempotency(payload.idempotency_key):
                    logger.info("[%s] Duplicate notification, skipping", correlation_id)
                    return

                if not payload.token or len(payload.token) < 10:
                    await self._handle_invalid_token(correlation_id, data, notification_id)
                    return

                if not notification_id:
                    notification = await self.db_manager.create_notification({**data, "status": "processing"})
                    notification_id = str(notification.id)
                else:
                    await self.db_manager.update_notification_status(notification_id, "processing")

                result = self.push_service.send_push_notification(payload, correlation_id)

                if notification_id:
                    await self.db_manager.update_notification_status(notification_id, "sent")

                if payload.idempotency_key:
                    await self.cache_manager.set_idempotency(payload.idempotency_key)

                logger.info("[%s] Notification sent successfully", correlation_id)
                return result

            except Exception as exc:
                await self._handle_processing_error(
                    correlation_id,
                    exc,
                    data,
                    notification_id,
                    message
                )

    async def _handle_invalid_token(self, correlation_id: str, data, notification_id: Optional[str]):
        logger.error("[%s] Invalid device token", correlation_id)
        if not notification_id:
            try:
                notification = await self.db_manager.create_notification(
                    {**data, "status": "failed", "error_message": "Invalid device token"}
                )
                notification_id = str(notification.id)
            except Exception as db_error:
                logger.error("[%s] Failed to store notification in DB: %s", correlation_id, db_error)
        else:
            await self.db_manager.update_notification_status(
                notification_id,
                "failed",
                error_message="Invalid device token"
            )

        await self.queue_manager.publish_message(
            self.queue_manager.dead_letter_queue,
            {**data, "error": "Invalid device token", "notification_id": notification_id}
        )

    async def _handle_processing_error(
        self,
        correlation_id: str,
        error: Exception,
        data: dict,
        notification_id: Optional[str],
        message: aio_pika.IncomingMessage
    ):
        logger.error("[%s] Error processing message: %s", correlation_id, error)
        retry_count = data.get("retry_count", 0) if data else 0

        if notification_id:
            try:
                await self.db_manager.update_notification_status(
                    notification_id,
                    "retrying" if retry_count < self.max_retries else "failed",
                    error_message=str(error),
                    retry_count=retry_count
                )
            except Exception as db_error:
                logger.error("[%s] Failed to update notification status: %s", correlation_id, db_error)

        if retry_count < self.max_retries:
            delay = (2 ** retry_count) * 5
            if data:
                data["retry_count"] = retry_count + 1
                data["notification_id"] = notification_id

            await asyncio.sleep(delay)
            await self.queue_manager.publish_message(
                self.queue_manager.retry_queue,
                data if data else {}
            )
            logger.info("[%s] Message requeued for retry %s", correlation_id, retry_count + 1)
        else:
            await self.queue_manager.publish_message(
                self.queue_manager.dead_letter_queue,
                {**(data if data else {}), "error": str(error), "notification_id": notification_id}
            )
            logger.error("[%s] Message moved to DLQ after %s retries", correlation_id, self.max_retries)

    async def start_consuming(self):
        push_queue = await self.queue_manager.channel.get_queue(self.queue_manager.push_queue)
        retry_queue = await self.queue_manager.channel.get_queue(self.queue_manager.retry_queue)

        await push_queue.consume(self.process_message)
        await retry_queue.consume(self.process_message)

        logger.info("Started consuming messages from push and retry queues")

