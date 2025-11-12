from fastapi import FastAPI, HTTPException, Query
from typing import Optional, Dict, Any, List
import asyncio
import aio_pika
import redis.asyncio as redis
import requests
import json
import os
import uuid
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from contextlib import asynccontextmanager
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select
from app.models import Notification, DeviceToken
from app.schemas import NotificationStatus, APIResponse, DeviceTokenRequest, BulkNotificationRequest, PushNotificationPayload

logging.basicConfig(
    filename="push_logs.log",
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"  
    
    def call(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failures = 0
                logger.info("Circuit breaker reset to CLOSED state")
            return result
        except Exception as e:
            self.failures += 1
            self.last_failure_time = datetime.now()
            
            if self.failures >= self.failure_threshold:
                self.state = "OPEN"
                logger.error(f"Circuit breaker opened after {self.failures} failures")
            raise e


class PushServiceManager:
    def __init__(self, service_account_path: str, project_id: str):
        self.service_account_path = service_account_path
        self.project_id = project_id
        self.credentials = None
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, timeout=60)
        self._init_credentials()
    
    def _init_credentials(self):
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                self.service_account_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            logger.info("Firebase credentials initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize credentials: {e}")
            raise
    
    def get_access_token(self):
        try:
            self.credentials.refresh(Request())
            return self.credentials.token
        except Exception as e:
            logger.error(f"Failed to refresh access token: {e}")
            raise
    
    def send_push_notification(self, payload: PushNotificationPayload, correlation_id: str):
        def _send():
            access_token = self.get_access_token()
            url = f"https://fcm.googleapis.com/v1/projects/{self.project_id}/messages:send"
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; UTF-8",
            }
            
            message = {
                "message": {
                    "token": payload.token,
                    "notification": {
                        "title": payload.title,
                        "body": payload.body,
                    }
                }
            }
            
            if payload.image:
                message["message"]["notification"]["image"] = payload.image
            
            if payload.link:
                message["message"]["webpush"] = {
                    "fcm_options": {
                        "link": payload.link
                    }
                }
            
            if payload.data:
                message["message"]["data"] = payload.data
            
            logger.info(f"[{correlation_id}] Sending notification to FCM")
            response = requests.post(url, headers=headers, json=message, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"[{correlation_id}] Notification sent successfully")
                return response.json()
            else:
                logger.error(f"[{correlation_id}] FCM error: {response.text}")
                raise Exception(f"FCM Error: {response.text}")
        
        return self.circuit_breaker.call(_send)


class MessageQueueManager:
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self.connection = None
        self.channel = None
        self.exchange_name = "notifications.direct"
        self.exchange = None
        self.push_queue = "push.queue"
        self.retry_queue = "push.queue.retry"
        self.dead_letter_queue = "failed.queue"
    
    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=10)
            
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
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
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


class CacheManager:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None
    
    async def connect(self):
        self.client = await redis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis successfully")
    
    async def check_idempotency(self, key: str) -> bool:
        """Check if notification was already processed"""
        exists = await self.client.exists(f"notification:{key}")
        return exists == 1
    
    async def set_idempotency(self, key: str, ttl: int = 86400):
        """Mark notification as processed"""
        await self.client.setex(f"notification:{key}", ttl, "1")
    
    async def close(self):
        if self.client:
            await self.client.close()

class DatabaseManager:
    def __init__(self, database_url: str):
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        self.database_url = database_url
        self.engine = None
        self.async_session = None
    
    async def connect(self):
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20
        )
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        logger.info("Connected to PostgreSQL successfully")
    
    async def create_notification(self, notification_data: Dict[str, Any]) -> Notification:
        async with self.async_session() as session:
            notification = Notification(
                idempotency_key=notification_data.get("idempotency_key"),
                user_id=notification_data.get("user_id"),
                device_token=notification_data.get("token"),
                notification_type=notification_data.get("notification_type", "mobile"),
                title=notification_data.get("title"),
                body=notification_data.get("body"),
                image_url=notification_data.get("image"),
                link_url=notification_data.get("link"),
                data=notification_data.get("data"),
                status=notification_data.get("status", "pending"),
                retry_count=notification_data.get("retry_count", 0)
            )
            session.add(notification)
            await session.commit()
            await session.refresh(notification)
            return notification
    
    async def get_notification_by_id(self, notification_id: str) -> Optional[Notification]:
        async with self.async_session() as session:
            result = await session.execute(
                select(Notification).where(Notification.id == uuid.UUID(notification_id))
            )
            return result.scalar_one_or_none()
    
    async def get_notification_by_idempotency_key(self, idempotency_key: str) -> Optional[Notification]:
        async with self.async_session() as session:
            result = await session.execute(
                select(Notification).where(Notification.idempotency_key == idempotency_key)
            )
            return result.scalar_one_or_none()
    
    async def update_notification_status(
        self, 
        notification_id: str, 
        status: str, 
        error_message: Optional[str] = None,
        retry_count: Optional[int] = None
    ):
        async with self.async_session() as session:
            result = await session.execute(
                select(Notification).where(Notification.id == uuid.UUID(notification_id))
            )
            notification = result.scalar_one_or_none()
            if notification:
                notification.status = status
                if error_message:
                    notification.error_message = error_message
                if retry_count is not None:
                    notification.retry_count = retry_count
                if status == "sent":
                    notification.sent_at = datetime.utcnow()
                await session.commit()
                return notification
            return None
    
    async def create_or_update_device_token(
        self, 
        user_id: str, 
        token: str, 
        device_type: str = "mobile",
        platform: Optional[str] = None
    ) -> DeviceToken:
        """Create or update device token for a user"""
        async with self.async_session() as session:
            result = await session.execute(
                select(DeviceToken).where(DeviceToken.token == token)
            )
            existing_token = result.scalar_one_or_none()
            
            if existing_token:
                existing_token.user_id = user_id
                existing_token.device_type = device_type
                existing_token.platform = platform
                existing_token.is_active = True
                existing_token.last_used_at = datetime.utcnow()
                await session.commit()
                await session.refresh(existing_token)
                return existing_token
            else:
                device_token = DeviceToken(
                    user_id=user_id,
                    token=token,
                    device_type=device_type,
                    platform=platform,
                    is_active=True
                )
                session.add(device_token)
                await session.commit()
                await session.refresh(device_token)
                return device_token
    
    async def get_device_tokens_by_user_id(self, user_id: str, active_only: bool = True) -> List[DeviceToken]:
        """Get all device tokens for a user"""
        async with self.async_session() as session:
            query = select(DeviceToken).where(DeviceToken.user_id == user_id)
            if active_only:
                query = query.where(DeviceToken.is_active == True)
            result = await session.execute(query)
            return list(result.scalars().all())
    
    async def deactivate_device_token(self, token: str) -> bool:
        """Deactivate a device token"""
        async with self.async_session() as session:
            result = await session.execute(
                select(DeviceToken).where(DeviceToken.token == token)
            )
            device_token = result.scalar_one_or_none()
            if device_token:
                device_token.is_active = False
                await session.commit()
                return True
            return False
    
    async def close(self):
        if self.engine:
            await self.engine.dispose()

class NotificationConsumer:
    def __init__(self, queue_manager: MessageQueueManager, 
                 cache_manager: CacheManager,
                 push_service: PushServiceManager,
                 db_manager: DatabaseManager):
        self.queue_manager = queue_manager
        self.cache_manager = cache_manager
        self.push_service = push_service
        self.db_manager = db_manager
        self.max_retries = 3
    
    async def process_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            correlation_id = message.correlation_id or str(uuid.uuid4())
            notification_id = None
            data = {}
            
            try:
                data = json.loads(message.body.decode())
                logger.info(f"[{correlation_id}] Processing notification: {data.get('title')}")
                
                notification_id = data.get("notification_id")
                
                payload = PushNotificationPayload(**data)
                
                if payload.idempotency_key:
                    if await self.cache_manager.check_idempotency(payload.idempotency_key):
                        logger.info(f"[{correlation_id}] Duplicate notification, skipping")
                        return
                
                if not payload.token or len(payload.token) < 10:
                    logger.error(f"[{correlation_id}] Invalid device token")
                    if not notification_id:
                        try:
                            notification_data = {
                                **data,
                                "status": "failed",
                                "error_message": "Invalid device token"
                            }
                            notification = await self.db_manager.create_notification(notification_data)
                            notification_id = str(notification.id)
                        except Exception as db_error:
                            logger.error(f"[{correlation_id}] Failed to store notification in DB: {db_error}")
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
                    return
                
                if not notification_id:
                    try:
                        notification_data = {
                            **data,
                            "status": "processing"
                        }
                        notification = await self.db_manager.create_notification(notification_data)
                        notification_id = str(notification.id)
                    except Exception as db_error:
                        logger.error(f"[{correlation_id}] Failed to store notification in DB: {db_error}")
                else:
                    await self.db_manager.update_notification_status(notification_id, "processing")
                
                result = self.push_service.send_push_notification(payload, correlation_id)
                
                if notification_id:
                    await self.db_manager.update_notification_status(notification_id, "sent")
                
                if payload.idempotency_key:
                    await self.cache_manager.set_idempotency(payload.idempotency_key)
                
                logger.info(f"[{correlation_id}] Notification sent successfully")
                
            except Exception as e:
                logger.error(f"[{correlation_id}] Error processing message: {e}")
                retry_count = data.get("retry_count", 0) if data else 0
                
                if notification_id:
                    try:
                        await self.db_manager.update_notification_status(
                            notification_id, 
                            "retrying" if retry_count < self.max_retries else "failed",
                            error_message=str(e),
                            retry_count=retry_count
                        )
                    except Exception as db_error:
                        logger.error(f"[{correlation_id}] Failed to update notification status: {db_error}")
                
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
                    logger.info(f"[{correlation_id}] Message requeued for retry {retry_count + 1}")
                else:
                    await self.queue_manager.publish_message(
                        self.queue_manager.dead_letter_queue,
                        {**(data if data else {}), "error": str(e), "notification_id": notification_id}
                    )
                    logger.error(f"[{correlation_id}] Message moved to DLQ after {self.max_retries} retries")
    
    async def start_consuming(self):
        push_queue = await self.queue_manager.channel.get_queue(self.queue_manager.push_queue)
        retry_queue = await self.queue_manager.channel.get_queue(self.queue_manager.retry_queue)
        
        await push_queue.consume(self.process_message)
        await retry_queue.consume(self.process_message)
        
        logger.info("Started consuming messages from push and retry queues")

queue_manager = None
cache_manager = None
push_service = None
consumer = None
db_manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global queue_manager, cache_manager, push_service, consumer, db_manager
    
    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
    database_url = os.getenv("DATABASE_URL", "postgresql://notif_user:notif_pass@postgres:5432/notifications_db")
    service_account_path = "firebase-credentials.json"
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
    health_status = {
        "service": "push-notification",
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "rabbitmq": "connected" if queue_manager and queue_manager.connection else "disconnected",
        "redis": "connected" if cache_manager and cache_manager.client else "disconnected",
        "circuit_breaker": push_service.circuit_breaker.state if push_service else "unknown"
    }
    
    is_healthy = all([
        queue_manager and queue_manager.connection,
        cache_manager and cache_manager.client
    ])
    
    if not is_healthy:
        return APIResponse(
            success=False,
            message="Service unhealthy",
            data=health_status
        ).dict()
    
    return APIResponse(
        success=True,
        message="Service healthy",
        data=health_status
    ).dict()

@app.post("/api/notifications/send")
async def send_notification(payload: PushNotificationPayload):
    """Queue a push notification for sending"""
    try:
        if not payload.idempotency_key:
            payload.idempotency_key = str(uuid.uuid4())
        
        if await cache_manager.check_idempotency(payload.idempotency_key):
            existing_notification = await db_manager.get_notification_by_idempotency_key(payload.idempotency_key)
            if existing_notification:
                return APIResponse(
                    success=True,
                    message="Notification already processed",
                    data={
                        "idempotency_key": payload.idempotency_key,
                        "notification_id": str(existing_notification.id),
                        "status": existing_notification.status
                    }
                ).dict()
            return APIResponse(
                success=True,
                message="Notification already processed",
                data={"idempotency_key": payload.idempotency_key}
            ).dict()
        
        notification_data = payload.dict()
        notification_data["status"] = "pending"
        notification_data["retry_count"] = 0
        notification = await db_manager.create_notification(notification_data)
        
        message = payload.dict()
        message["retry_count"] = 0
        message["queued_at"] = datetime.utcnow().isoformat()
        message["notification_id"] = str(notification.id)
        
        await queue_manager.publish_message(
            queue_manager.push_queue,
            message
        )
        
        logger.info(f"Notification queued: {payload.idempotency_key}")
        
        return APIResponse(
            success=True,
            message="Notification queued successfully",
            data={
                "idempotency_key": payload.idempotency_key,
                "notification_id": str(notification.id),
                "status": NotificationStatus.PENDING
            }
        ).dict()
        
    except Exception as e:
        logger.error(f"Error queuing notification: {e}")
        return APIResponse(
            success=False,
            message="Failed to queue notification",
            error="internal_error"
        ).dict()

@app.post("/api/notifications/send-immediate")
async def send_immediate_notification(payload: PushNotificationPayload):
    """Send push notification immediately (synchronous)"""
    correlation_id = str(uuid.uuid4())
    
    try:
        if payload.idempotency_key:
            if await cache_manager.check_idempotency(payload.idempotency_key):
                existing_notification = await db_manager.get_notification_by_idempotency_key(payload.idempotency_key)
                if existing_notification:
                    return APIResponse(
                        success=True,
                        message="Notification already processed",
                        data={
                            "idempotency_key": payload.idempotency_key,
                            "notification_id": str(existing_notification.id),
                            "status": existing_notification.status
                        }
                    ).dict()
        
        notification_data = payload.dict()
        notification_data["status"] = "processing"
        notification = await db_manager.create_notification(notification_data)
        
        result = push_service.send_push_notification(payload, correlation_id)
        
        await db_manager.update_notification_status(str(notification.id), "sent")
        
        if payload.idempotency_key:
            await cache_manager.set_idempotency(payload.idempotency_key)
        
        return APIResponse(
            success=True,
            message="Notification sent successfully",
            data={
                **result,
                "notification_id": str(notification.id),
                "idempotency_key": payload.idempotency_key
            }
        ).dict()
        
    except Exception as e:
        logger.error(f"[{correlation_id}] Error sending notification: {e}")
        if 'notification' in locals():
            try:
                await db_manager.update_notification_status(
                    str(notification.id), 
                    "failed",
                    error_message=str(e)
                )
            except:
                pass
        
        return APIResponse(
            success=False,
            message="Failed to send notification",
            error="internal_error"
        ).dict()

@app.get("/api/notifications/status")
async def get_notification_status(
    idempotency_key: Optional[str] = Query(None, description="Get status by idempotency key"),
    notification_id: Optional[str] = Query(None, description="Get status by notification ID")
):
    """Get the status of a notification by idempotency_key or notification_id"""
    try:
        if not idempotency_key and not notification_id:
            raise HTTPException(
                status_code=400,
                detail="Either idempotency_key or notification_id must be provided"
            )
        
        notification = None
        if notification_id:
            try:
                notification = await db_manager.get_notification_by_id(notification_id)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid notification_id format")
        elif idempotency_key:
            notification = await db_manager.get_notification_by_idempotency_key(idempotency_key)
        
        if not notification:
            return APIResponse(
                success=False,
                message="Notification not found",
                error="No notification found with the provided identifier"
            ).dict()
        
        device_token = notification.device_token
        sent_at = notification.sent_at
        created_at = notification.created_at
        updated_at = notification.updated_at
        
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
            "updated_at": updated_at.isoformat() if updated_at else None
        }
        
        return APIResponse(
            success=True,
            message="Notification status retrieved successfully",
            data=notification_data
        ).dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving notification status: {e}")
        return APIResponse(
            success=False,
            message="Failed to retrieve notification status",
            error="internal_error"
        ).dict()

@app.post("/api/notifications/send-bulk")
async def send_bulk_notifications(request: BulkNotificationRequest):
    """Send multiple push notifications in bulk"""
    try:
        if not request.notifications:
            raise HTTPException(status_code=400, detail="At least one notification is required")
        
        if len(request.notifications) > 100:
            raise HTTPException(status_code=400, detail="Maximum 100 notifications allowed per request")
        
        results = []
        queued_count = 0
        duplicate_count = 0
        error_count = 0
        
        for idx, payload in erate(request.notifications):
            try:
                if not payload.idempotency_key:
                    payload.idempotency_key = str(uuid.uuid4())
                
                is_duplicate = await cache_manager.check_idempotency(payload.idempotency_key)
                if is_duplicate:
                    existing_notification = await db_manager.get_notification_by_idempotency_key(payload.idempotency_key)
                    results.append({
                        "index": idx,
                        "idempotency_key": payload.idempotency_key,
                        "notification_id": str(existing_notification.id) if existing_notification else None,
                        "status": "duplicate",
                        "message": "Notification already processed"
                    })
                    duplicate_count += 1
                    continue
                
                notification_data = payload.dict()
                notification_data["status"] = "pending"
                notification_data["retry_count"] = 0
                notification = await db_manager.create_notification(notification_data)
                
                message = payload.dict()
                message["retry_count"] = 0
                message["queued_at"] = datetime.utcnow().isoformat()
                message["notification_id"] = str(notification.id)
                
                await queue_manager.publish_message(
                    queue_manager.push_queue,
                    message
                )
                
                results.append({
                    "index": idx,
                    "idempotency_key": payload.idempotency_key,
                    "notification_id": str(notification.id),
                    "status": "queued",
                    "message": "Notification queued successfully"
                })
                queued_count += 1
                
            except Exception as e:
                logger.error(f"Error queuing notification at index {idx}: {e}")
                results.append({
                    "index": idx,
                    "idempotency_key": payload.idempotency_key if payload.idempotency_key else None,
                    "status": "error",
                    "message": "Failed to queue notification",
                    "error": "internal_error"
                })
                error_count += 1
        
        return APIResponse(
            success=True,
            message=f"Bulk notification processing completed. Queued: {queued_count}, Duplicates: {duplicate_count}, Errors: {error_count}",
            data={
                "total": len(request.notifications),
                "queued": queued_count,
                "duplicates": duplicate_count,
                "errors": error_count,
                "results": results
            }
        ).dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing bulk notifications: {e}")
        return APIResponse(
            success=False,
            message="Failed to process bulk notifications",
            error="internal_error"
        ).dict()

@app.post("/api/device-tokens")
async def register_device_token(request: DeviceTokenRequest):
    """Register or update an FCM device token for a user"""
    try:
        if not request.token or len(request.token) < 10:
            raise HTTPException(status_code=400, detail="Invalid device token")
        
        if not request.user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        device_token = await db_manager.create_or_update_device_token(
            user_id=request.user_id,
            token=request.token,
            device_type=request.device_type,
            platform=request.platform
        )
        
        return APIResponse(
            success=True,
            message="Device token registered successfully",
            data={
                "id": str(device_token.id),
                "user_id": device_token.user_id,
                "token": device_token.token[:20] + "..." if len(device_token.token) > 20 else device_token.token,
                "device_type": device_token.device_type,
                "platform": device_token.platform,
                "is_active": device_token.is_active,
                "created_at": device_token.created_at.isoformat() if device_token.created_at else None,
                "updated_at": device_token.updated_at.isoformat() if device_token.updated_at else None
            }
        ).dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering device token: {e}")
        return APIResponse(
            success=False,
            message="Failed to register device token",
            error="internal_error"
        ).dict()

@app.get("/api/device-tokens/{user_id}")
async def get_user_device_tokens(user_id: str, active_only: bool = True):
    """Get all device tokens for a user"""
    try:
        device_tokens = await db_manager.get_device_tokens_by_user_id(user_id, active_only=active_only)
        
        tokens_data = []
        for token in device_tokens:
            tokens_data.append({
                "id": str(token.id),
                "user_id": token.user_id,
                "token": token.token[:20] + "..." if len(token.token) > 20 else token.token,
                "device_type": token.device_type,
                "platform": token.platform,
                "is_active": token.is_active,
                "last_used_at": token.last_used_at.isoformat() if token.last_used_at else None,
                "created_at": token.created_at.isoformat() if token.created_at else None,
                "updated_at": token.updated_at.isoformat() if token.updated_at else None
            })
        
        return APIResponse(
            success=True,
            message=f"Retrieved {len(tokens_data)} device token(s)",
            data=tokens_data
        ).dict()
        
    except Exception as e:
        logger.error(f"Error retrieving device tokens: {e}")
        return APIResponse(
            success=False,
            message="Failed to retrieve device tokens",
            error="internal_error"
        ).dict()

@app.delete("/api/device-tokens/{token}")
async def deactivate_device_token(token: str):
    """Deactivate a device token"""
    try:
        success = await db_manager.deactivate_device_token(token)
        
        if not success:
            return APIResponse(
                success=False,
                message="Device token not found",
                error="No device token found with the provided token"
            ).dict()
        
        return APIResponse(
            success=True,
            message="Device token deactivated successfully"
        ).dict()
        
    except Exception as e:
        logger.error(f"Error deactivating device token: {e}")
        return APIResponse(
            success=False,
            message="Failed to deactivate device token",
            error="internal_error"
        ).dict()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
