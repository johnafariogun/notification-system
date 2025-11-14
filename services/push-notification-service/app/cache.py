import logging
from typing import Optional

import redis.asyncio as redis


logger = logging.getLogger(__name__)


class CacheManager:
    """Handles Redis-based idempotency checks."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None

    async def connect(self):
        self.client = await redis.from_url(self.redis_url, decode_responses=True)
        logger.info("Connected to Redis successfully")

    async def check_idempotency(self, key: str) -> bool:
        exists = await self.client.exists(f"notification:{key}")
        return exists == 1

    async def set_idempotency(self, key: str, ttl: int = 86400):
        await self.client.setex(f"notification:{key}", ttl, "1")

    async def close(self):
        if self.client:
            await self.client.close()

