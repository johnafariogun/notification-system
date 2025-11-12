"""Database models"""
from sqlalchemy import Column, String, Boolean, Integer, Text, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import uuid

Base = declarative_base()

class Notification(Base):
    __tablename__ = "notifications"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key = Column(String(255), unique=True, index=True)
    user_id = Column(String(255), index=True)
    device_token = Column(String(512), nullable=False)
    notification_type = Column(String(50), nullable=False)
    title = Column(String(255), nullable=False)
    body = Column(Text, nullable=False)
    image_url = Column(Text)
    link_url = Column(Text)
    data = Column(JSONB)
    status = Column(String(50), nullable=False, default='pending', index=True)
    retry_count = Column(Integer, default=0)
    error_message = Column(Text)
    sent_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, default=func.now(), index=True)
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())

class DeviceToken(Base):
    __tablename__ = "device_tokens"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(String(255), nullable=False, index=True)
    token = Column(String(512), nullable=False, unique=True, index=True)
    device_type = Column(String(50), nullable=False)
    platform = Column(String(50))
    is_active = Column(Boolean, default=True, index=True)
    last_used_at = Column(TIMESTAMP, default=func.now())
    created_at = Column(TIMESTAMP, default=func.now())
    updated_at = Column(TIMESTAMP, default=func.now(), onupdate=func.now())
