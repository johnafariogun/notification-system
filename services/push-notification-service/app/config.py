"""Configuration management"""
import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    redis_url: str = "redis://redis:6379"
    database_url: str = "postgresql://notif_user:notif_pass@postgres:5432/notifications_db"
    service_account_path: str = "/app/firebase-credentials.json"
    project_id: str = "mindful-torus-458106-p9"
    log_level: str = "INFO"
    service_port: int = 8001
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    
    rate_limit_per_minute: int = 1000
    rate_limit_per_hour: int = 10000
    
    worker_prefetch_count: int = 10
    worker_threads: int = 4
    
    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
