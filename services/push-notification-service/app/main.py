import asyncio
import logging
import os
import uuid
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional, cast

from fastapi import FastAPI, HTTPException, Query

from app.cache import CacheManager
from app.consumer import NotificationConsumer
from app.database import DatabaseManager
from app.queue import MessageQueueManager
from app.push import PushServiceManager
from app.schemas import (
    APIResponse,
    BulkNotificationRequest,
    DeviceTokenRequest,
    NotificationStatus,
    PushNotificationPayload,
)

logging.basicConfig(
    filename="push_logs.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


queue_manager: Optional[MessageQueueManager] = None
cache_manager: Optional[CacheManager] = None
push_service: Optional[PushServiceManager] = None
consumer: Optional[NotificationConsumer] = None
db_manager: Optional[DatabaseManager] = None


def get_queue_manager() -> MessageQueueManager:
    if queue_manager is None:
        raise RuntimeError("Queue manager is not initialized")
    return queue_manager


def get_cache_manager() -> CacheManager:
    if cache_manager is None:
        raise RuntimeError("Cache manager is not initialized")
    return cache_manager


def get_db_manager() -> DatabaseManager:
    if db_manager is None:
        raise RuntimeError("Database manager is not initialized")
    return db_manager


def get_push_service() -> PushServiceManager:
    if push_service is None:
        raise RuntimeError("Push service is not initialized")
    return push_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    global queue_manager, cache_manager, push_service, consumer, db_manager

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://notif_user:notif_pass@postgres:5432/notifications_db",
    )
    service_account_path = os.getenv("SERVICE_ACCOUNT_PATH", "firebase-credentials.json")
    project_id = os.getenv("PROJECT_ID", "mindful-torus-458106-p9")

    queue_manager = MessageQueueManager(rabbitmq_url)
    await queue_manager.connect()

    cache_manager = CacheManager(redis_url)
    await cache_manager.connect()

    db_manager = DatabaseManager(database_url)
    await db_manager.connect()

    push_service = PushServiceManager(service_account_path, project_id)

    consumer = NotificationConsumer(queue_manager, cache_manager, push_service, db_manager)
    asyncio.create_task(consumer.start_consuming())

    logger.info("Push Notification Service started successfully")

    yield

    await queue_manager.close()
    await cache_manager.close()
    await db_manager.close()
    logger.info("Push Notification Service shut down")


app = FastAPI(title="Push Notification Service", lifespan=lifespan)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    queue = get_queue_manager()
    cache = get_cache_manager()
    push = get_push_service()

    health_status = {
        "service": "push-notification",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "rabbitmq": "connected" if queue.connection else "disconnected",
        "redis": "connected" if cache.client else "disconnected",
        "circuit_breaker": push.circuit_breaker.state,
    }

    is_healthy = all(
        [
            queue.connection,
            cache.client,
        ]
    )

    if not is_healthy:
        return APIResponse(
            success=False,
            message="Service unhealthy",
            data=health_status,
        ).dict()

    return APIResponse(
        success=True,
        message="Service healthy",
        data=health_status,
    ).dict()


@app.post("/api/notifications/send")
async def send_notification(payload: PushNotificationPayload):
    """Queue a push notification for sending"""
    try:
        cache = get_cache_manager()
        db = get_db_manager()
        queue = get_queue_manager()

        if not payload.idempotency_key:
            payload.idempotency_key = str(uuid.uuid4())

        if await cache.check_idempotency(payload.idempotency_key):
            existing_notification = await db.get_notification_by_idempotency_key(
                payload.idempotency_key
            )
            if existing_notification:
                return APIResponse(
                    success=True,
                    message="Notification already processed",
                    data={
                        "idempotency_key": payload.idempotency_key,
                        "notification_id": str(existing_notification.id),
                        "status": existing_notification.status,
                    },
                ).dict()
            return APIResponse(
                success=True,
                message="Notification already processed",
                data={"idempotency_key": payload.idempotency_key},
            ).dict()

        notification_data = payload.dict()
        notification_data["status"] = "pending"
        notification_data["retry_count"] = 0
        notification = await db.create_notification(notification_data)

        message = payload.dict()
        message["retry_count"] = 0
        message["queued_at"] = datetime.utcnow().isoformat()
        message["notification_id"] = str(notification.id)

        await queue.publish_message(
            queue.push_queue,
            message,
        )

        logger.info("Notification queued: %s", payload.idempotency_key)

        return APIResponse(
            success=True,
            message="Notification queued successfully",
            data={
                "idempotency_key": payload.idempotency_key,
                "notification_id": str(notification.id),
                "status": NotificationStatus.PENDING,
            },
        ).dict()

    except Exception as exc:
        logger.error("Error queuing notification: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to queue notification",
            error="internal_error",
        ).dict()


@app.post("/api/notifications/send-immediate")
async def send_immediate_notification(payload: PushNotificationPayload):
    """Send push notification immediately (synchronous)"""
    correlation_id = str(uuid.uuid4())

    cache = get_cache_manager()
    db = get_db_manager()
    push = get_push_service()
    notification = None

    try:
        if payload.idempotency_key:
            if await cache.check_idempotency(payload.idempotency_key):
                existing_notification = await db.get_notification_by_idempotency_key(
                    payload.idempotency_key
                )
                if existing_notification:
                    return APIResponse(
                        success=True,
                        message="Notification already processed",
                        data={
                            "idempotency_key": payload.idempotency_key,
                            "notification_id": str(existing_notification.id),
                            "status": existing_notification.status,
                        },
                    ).dict()

        notification_data = payload.dict()
        notification_data["status"] = "processing"
        notification = await db.create_notification(notification_data)

        result = push.send_push_notification(payload, correlation_id)

        await db.update_notification_status(str(notification.id), "sent")

        if payload.idempotency_key:
            await cache.set_idempotency(payload.idempotency_key)

        return APIResponse(
            success=True,
            message="Notification sent successfully",
            data={
                **result,
                "notification_id": str(notification.id),
                "idempotency_key": payload.idempotency_key,
            },
        ).dict()

    except Exception as exc:
        logger.error("[%s] Error sending notification: %s", correlation_id, exc)
        if notification is not None:
            try:
                await db.update_notification_status(
                    str(notification.id),
                    "failed",
                    error_message=str(exc),
                )
            except Exception:  # noqa: BLE001
                pass

        return APIResponse(
            success=False,
            message="Failed to send notification",
            error="internal_error",
        ).dict()


@app.get("/api/notifications/status")
async def get_notification_status(
    idempotency_key: Optional[str] = Query(None, description="Get status by idempotency key"),
    notification_id: Optional[str] = Query(None, description="Get status by notification ID"),
):
    """Get the status of a notification by idempotency_key or notification_id"""
    try:
        db = get_db_manager()

        if not idempotency_key and not notification_id:
            raise HTTPException(
                status_code=400,
                detail="Either idempotency_key or notification_id must be provided",
            )

        notification = None
        if notification_id:
            try:
                notification = await db.get_notification_by_id(notification_id)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid notification_id format")
        elif idempotency_key:
            notification = await db.get_notification_by_idempotency_key(idempotency_key)

        if not notification:
            return APIResponse(
                success=False,
                message="Notification not found",
                error="No notification found with the provided identifier",
            ).dict()

        device_token = cast(Optional[str], notification.device_token)
        sent_at = cast(Optional[datetime], notification.sent_at)
        created_at = cast(Optional[datetime], notification.created_at)
        updated_at = cast(Optional[datetime], notification.updated_at)

        notification_data = {
            "id": str(notification.id),
            "idempotency_key": notification.idempotency_key,
            "user_id": notification.user_id,
            "device_token": (device_token[:20] + "...") if device_token and len(device_token) > 20 else device_token,
            "notification_type": notification.notification_type,
            "title": notification.title,
            "body": notification.body,
            "image_url": notification.image_url,
            "link_url": notification.link_url,
            "data": notification.data,
            "status": notification.status,
            "retry_count": notification.retry_count,
            "error_message": notification.error_message,
            "sent_at": sent_at.isoformat() if sent_at else None,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }

        return APIResponse(
            success=True,
            message="Notification status retrieved successfully",
            data=notification_data,
        ).dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error retrieving notification status: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to retrieve notification status",
            error="internal_error",
        ).dict()


@app.post("/api/notifications/send-bulk")
async def send_bulk_notifications(request: BulkNotificationRequest):
    """Send multiple push notifications in bulk"""
    try:
        cache = get_cache_manager()
        db = get_db_manager()
        queue = get_queue_manager()

        if not request.notifications:
            raise HTTPException(status_code=400, detail="At least one notification is required")

        if len(request.notifications) > 100:
            raise HTTPException(status_code=400, detail="Maximum 100 notifications allowed per request")

        results = []
        queued_count = 0
        duplicate_count = 0
        error_count = 0

        for idx, payload in enumerate(request.notifications):
            try:
                if not payload.idempotency_key:
                    payload.idempotency_key = str(uuid.uuid4())

                is_duplicate = await cache.check_idempotency(payload.idempotency_key)
                if is_duplicate:
                    existing_notification = await db.get_notification_by_idempotency_key(
                        payload.idempotency_key
                    )
                    results.append(
                        {
                            "index": idx,
                            "idempotency_key": payload.idempotency_key,
                            "notification_id": str(existing_notification.id)
                            if existing_notification
                            else None,
                            "status": "duplicate",
                            "message": "Notification already processed",
                        }
                    )
                    duplicate_count += 1
                    continue

                notification_data = payload.dict()
                notification_data["status"] = "pending"
                notification_data["retry_count"] = 0
                notification = await db.create_notification(notification_data)

                message = payload.dict()
                message["retry_count"] = 0
                message["queued_at"] = datetime.utcnow().isoformat()
                message["notification_id"] = str(notification.id)

                await queue.publish_message(
                    queue.push_queue,
                    message,
                )

                results.append(
                    {
                        "index": idx,
                        "idempotency_key": payload.idempotency_key,
                        "notification_id": str(notification.id),
                        "status": "queued",
                        "message": "Notification queued successfully",
                    }
                )
                queued_count += 1

            except Exception as exc:
                logger.error("Error queuing notification at index %s: %s", idx, exc)
                results.append(
                    {
                        "index": idx,
                        "idempotency_key": payload.idempotency_key if payload.idempotency_key else None,
                        "status": "error",
                        "message": "Failed to queue notification",
                        "error": "internal_error",
                    }
                )
                error_count += 1

        return APIResponse(
            success=True,
            message=(
                f"Bulk notification processing completed. "
                f"Queued: {queued_count}, Duplicates: {duplicate_count}, Errors: {error_count}"
            ),
            data={
                "total": len(request.notifications),
                "queued": queued_count,
                "duplicates": duplicate_count,
                "errors": error_count,
                "results": results,
            },
        ).dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error processing bulk notifications: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to process bulk notifications",
            error="internal_error",
        ).dict()


@app.post("/api/device-tokens")
async def register_device_token(request: DeviceTokenRequest):
    """Register or update an FCM device token for a user"""
    try:
        db = get_db_manager()

        if not request.token or len(request.token) < 10:
            raise HTTPException(status_code=400, detail="Invalid device token")

        if not request.user_id:
            raise HTTPException(status_code=400, detail="user_id is required")

        device_token = await db.create_or_update_device_token(
            user_id=request.user_id,
            token=request.token,
            device_type=request.device_type,
            platform=request.platform,
        )

        token_value = cast(str, device_token.token)
        created_at = cast(Optional[datetime], device_token.created_at)
        updated_at = cast(Optional[datetime], device_token.updated_at)

        return APIResponse(
            success=True,
            message="Device token registered successfully",
            data={
                "id": str(device_token.id),
                "user_id": device_token.user_id,
                "token": token_value[:20] + "..." if len(token_value) > 20 else token_value,
                "device_type": device_token.device_type,
                "platform": device_token.platform,
                "is_active": device_token.is_active,
                "created_at": created_at.isoformat() if created_at else None,
                "updated_at": updated_at.isoformat() if updated_at else None,
            },
        ).dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error registering device token: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to register device token",
            error="internal_error",
        ).dict()


@app.get("/api/device-tokens/{user_id}")
async def get_user_device_tokens(user_id: str, active_only: bool = True):
    """Get all device tokens for a user"""
    try:
        db = get_db_manager()
        device_tokens = await db.get_device_tokens_by_user_id(user_id, active_only=active_only)

        tokens_data = []
        for token in device_tokens:
            token_value = cast(str, token.token)
            last_used_at = cast(Optional[datetime], token.last_used_at)
            created_at = cast(Optional[datetime], token.created_at)
            updated_at = cast(Optional[datetime], token.updated_at)
            tokens_data.append(
                {
                    "id": str(token.id),
                    "user_id": token.user_id,
                    "token": token_value[:20] + "..." if len(token_value) > 20 else token_value,
                    "device_type": token.device_type,
                    "platform": token.platform,
                    "is_active": token.is_active,
                    "last_used_at": last_used_at.isoformat() if last_used_at else None,
                    "created_at": created_at.isoformat() if created_at else None,
                    "updated_at": updated_at.isoformat() if updated_at else None,
                }
            )

        return APIResponse(
            success=True,
            message=f"Retrieved {len(tokens_data)} device token(s)",
            data=tokens_data,
        ).dict()

    except Exception as exc:
        logger.error("Error retrieving device tokens: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to retrieve device tokens",
            error="internal_error",
        ).dict()


@app.delete("/api/device-tokens/{token}")
async def deactivate_device_token(token: str):
    """Deactivate a device token"""
    try:
        db = get_db_manager()
        success = await db.deactivate_device_token(token)

        if not success:
            return APIResponse(
                success=False,
                message="Device token not found",
                error="No device token found with the provided token",
            ).dict()

        return APIResponse(
            success=True,
            message="Device token deactivated successfully",
        ).dict()

    except Exception as exc:
        logger.error("Error deactivating device token: %s", exc)
        return APIResponse(
            success=False,
            message="Failed to deactivate device token",
            error="internal_error",
        ).dict()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)

