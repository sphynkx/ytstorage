import json
import redis.asyncio as redis
from typing import Optional, Any
from config import redis_cfg
from drivers.driver_base_drv import FileStat
from utils import logging_ut

logger = logging_ut.get_logger("cache_ut")

class CacheManager:
    def __init__(self):
        self.enabled = redis_cfg.USE_REDIS_CACHE
        self.redis: Optional[redis.Redis] = None
        if self.enabled:
            try:
                self.redis = redis.from_url(redis_cfg.REDIS_URL, decode_responses=False) # Bytes mode
                logger.info(f"Redis Cache initialized: {redis_cfg.REDIS_URL}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.enabled = False

    async def close(self):
        if self.redis:
            await self.redis.close()

    def _key_stat(self, rel_path: str) -> str:
        return f"ytstorage:stat:{rel_path}"

    def _key_data(self, rel_path: str) -> str:
        return f"ytstorage:data:{rel_path}"

    # --- Metadata (Stat) ---

    async def get_stat(self, rel_path: str) -> Optional[FileStat]:
        if not self.enabled or not self.redis: return None
        try:
            data = await self.redis.get(self._key_stat(rel_path))
            if data:
                # Deserialize JSON to FileStat
                d = json.loads(data)
                return FileStat(**d)
        except Exception as e:
            logger.warning(f"Redis get_stat error: {e}")
        return None

    async def set_stat(self, rel_path: str, stat: FileStat):
        if not self.enabled or not self.redis: return
        try:
            # Serialize FileStat to JSON
            val = json.dumps(stat.__dict__)
            await self.redis.set(self._key_stat(rel_path), val, ex=redis_cfg.CACHE_TTL_META)
        except Exception as e:
            logger.warning(f"Redis set_stat error: {e}")

    # --- Data (Small Files) ---

    async def get_file_data(self, rel_path: str) -> Optional[bytes]:
        if not self.enabled or not self.redis: return None
        try:
            return await self.redis.get(self._key_data(rel_path))
        except Exception as e:
            logger.warning(f"Redis get_data error: {e}")
        return None

    async def set_file_data(self, rel_path: str, data: bytes):
        """Cache file content ONLY if it is small enough."""
        if not self.enabled or not self.redis: return
        if len(data) > redis_cfg.CACHE_MAX_FILE_SIZE: return 
        
        try:
            await self.redis.set(self._key_data(rel_path), data, ex=redis_cfg.CACHE_TTL_DATA)
        except Exception as e:
            logger.warning(f"Redis set_data error: {e}")

    # --- Invalidation ---

    async def invalidate(self, rel_path: str):
        """Clear both stat and data caches for a path."""
        if not self.enabled or not self.redis: return
        try:
            # Delete both potential keys
            await self.redis.delete(self._key_stat(rel_path), self._key_data(rel_path))
        except Exception as e:
            logger.warning(f"Redis invalidate error: {e}")

# Global singleton
cache_manager = CacheManager()