from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field

class NotificationStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"
    RETRYING = "retrying"

class NotificationType(str, Enum):
    MOBILE = "mobile"
    WEB = "web"

class PushNotificationPayload(BaseModel):
    title: str
    body: str
    token: str
    notification_type: NotificationType = NotificationType.MOBILE
    image: Optional[str] = None
    link: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None
    user_id: Optional[str] = None

class BulkNotificationRequest(BaseModel):
    notifications: List[PushNotificationPayload] = Field(..., min_length=1, max_length=100)

class DeviceTokenRequest(BaseModel):
    user_id: str
    token: str
    device_type: str = "mobile"
    platform: Optional[str] = None

class PaginationMeta(BaseModel):
    total: int
    limit: int
    page: int
    total_pages: int
    has_next: bool
    has_previous: bool

class APIResponse(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    message: str
    meta: Optional[PaginationMeta] = None
