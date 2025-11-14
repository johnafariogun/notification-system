import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import DeviceToken, Notification


logger = logging.getLogger(__name__)


class DatabaseManager:
    """Encapsulates async SQLAlchemy access for notifications and device tokens."""

    def __init__(self, database_url: str):
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        self.database_url = database_url
        self.engine = None
        self.async_session: Optional[async_sessionmaker] = None

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
        if self.async_session is None:
            raise RuntimeError("DatabaseManager not connected. Call connect() first.")
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
        async with self.async_session() as session:
            query = select(DeviceToken).where(DeviceToken.user_id == user_id)
            if active_only:
                query = query.where(DeviceToken.is_active == True)
            result = await session.execute(query)
            return list(result.scalars().all())

    async def deactivate_device_token(self, token: str) -> bool:
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

