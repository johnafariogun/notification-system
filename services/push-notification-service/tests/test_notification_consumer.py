import asyncio
import json
import uuid
from types import SimpleNamespace

import pytest

from app.main import NotificationConsumer, PushNotificationPayload


class FakeProcessContext:
    def __init__(self, message):
        self._message = message

    async def __aenter__(self):
        return self._message

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._message.acked = True
        return False


class FakeMessage:
    def __init__(self, body_dict, correlation_id=None):
        self.body = json.dumps(body_dict).encode()
        self.correlation_id = correlation_id
        self.acked = False

    def process(self):
        return FakeProcessContext(self)


class StubQueueManager:
    def __init__(self):
        self.push_queue = "push.queue"
        self.retry_queue = "push.queue.retry"
        self.dead_letter_queue = "failed.queue"
        self.published = []

    async def publish_message(self, routing_key, message):
        self.published.append((routing_key, message))


class StubCacheManager:
    def __init__(self, existing_keys=None):
        self.existing_keys = set(existing_keys or [])
        self.set_keys = []

    async def check_idempotency(self, key):
        return key in self.existing_keys

    async def set_idempotency(self, key, ttl=86400):
        self.set_keys.append((key, ttl))


class StubPushService:
    def __init__(self):
        self.calls = []

    def send_push_notification(self, payload, correlation_id):
        self.calls.append((payload, correlation_id))
        return {"name": "mock-message-id"}


class StubNotification:
    def __init__(self, **kwargs):
        self.id = uuid.uuid4()
        for key, value in kwargs.items():
            setattr(self, key, value)


class StubDatabaseManager:
    def __init__(self):
        self.created = []
        self.updated = []

    async def create_notification(self, notification_data):
        notification = StubNotification(**notification_data)
        self.created.append(notification)
        return notification

    async def update_notification_status(self, notification_id, status, error_message=None, retry_count=None):
        self.updated.append(
            SimpleNamespace(
                id=notification_id,
                status=status,
                error_message=error_message,
                retry_count=retry_count,
            )
        )


@pytest.mark.asyncio
async def test_process_message_successful_flow():
    queue_manager = StubQueueManager()
    cache_manager = StubCacheManager()
    push_service = StubPushService()
    db_manager = StubDatabaseManager()

    consumer = NotificationConsumer(
        queue_manager=queue_manager,
        cache_manager=cache_manager,
        push_service=push_service,
        db_manager=db_manager,
    )

    payload = {
        "title": "Hello",
        "body": "Test notification",
        "token": "valid-token-123456789",
        "notification_type": "mobile",
        "idempotency_key": "idem-key-1",
    }

    message = FakeMessage(payload, correlation_id="corr-123")

    await consumer.process_message(message)

    assert message.acked is True
    assert len(db_manager.created) == 1
    assert db_manager.updated[-1].status == "sent"
    assert cache_manager.set_keys == [("idem-key-1", 86400)]
    assert queue_manager.published == []
    assert push_service.calls[0][0] == PushNotificationPayload(**payload)


@pytest.mark.asyncio
async def test_process_message_invalid_token():
    queue_manager = StubQueueManager()
    cache_manager = StubCacheManager()
    push_service = StubPushService()
    db_manager = StubDatabaseManager()

    consumer = NotificationConsumer(
        queue_manager=queue_manager,
        cache_manager=cache_manager,
        push_service=push_service,
        db_manager=db_manager,
    )

    payload = {
        "title": "Hello",
        "body": "Test notification",
        "token": "short",
        "notification_type": "mobile",
        "idempotency_key": "idem-key-2",
    }

    message = FakeMessage(payload, correlation_id="corr-456")

    await consumer.process_message(message)

    assert len(queue_manager.published) == 1
    routing_key, published_message = queue_manager.published[0]
    assert routing_key == queue_manager.dead_letter_queue
    assert published_message["error"] == "Invalid device token"
    assert "notification_id" in published_message
    assert push_service.calls == []
    assert cache_manager.set_keys == []


@pytest.mark.asyncio
async def test_process_message_duplicate_idempotency_key_skips_processing():
    queue_manager = StubQueueManager()
    cache_manager = StubCacheManager(existing_keys={"idem-key-3"})
    push_service = StubPushService()
    db_manager = StubDatabaseManager()

    consumer = NotificationConsumer(
        queue_manager=queue_manager,
        cache_manager=cache_manager,
        push_service=push_service,
        db_manager=db_manager,
    )

    payload = {
        "title": "Hello",
        "body": "Duplicate notification",
        "token": "valid-token-987654321",
        "notification_type": "mobile",
        "idempotency_key": "idem-key-3",
    }

    message = FakeMessage(payload, correlation_id="corr-789")

    await consumer.process_message(message)

    assert db_manager.created == []
    assert push_service.calls == []
    assert queue_manager.published == []
    assert cache_manager.set_keys == []

