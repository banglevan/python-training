"""
Cache management implementation.
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
import json
import hashlib
import redis
from redis.cluster import RedisCluster
from redis.exceptions import LockError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CacheManager:
    """Manages distributed caching."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize cache manager."""
        self.config = config
        self.default_ttl = config.get('default_ttl', 3600)  # 1 hour
        
        if config.get('cluster_mode', False):
            self.redis = RedisCluster(
                startup_nodes=config['nodes'],
                decode_responses=True,
                **config.get('options', {})
            )
        else:
            self.redis = redis.Redis(
                host=config.get('host', 'localhost'),
                port=config.get('port', 6379),
                db=config.get('db', 0),
                decode_responses=True,
                **config.get('options', {})
            )
    
    def get(
        self,
        key: str,
        default: Any = None
    ) -> Optional[Any]:
        """Get value from cache."""
        try:
            data = self.redis.get(key)
            return json.loads(data) if data else default
            
        except Exception as e:
            logger.error(f"Cache get failed: {e}")
            return default
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """Set value in cache."""
        try:
            return self.redis.setex(
                key,
                ttl or self.default_ttl,
                json.dumps(value)
            )
            
        except Exception as e:
            logger.error(f"Cache set failed: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        try:
            return bool(self.redis.delete(key))
            
        except Exception as e:
            logger.error(f"Cache delete failed: {e}")
            return False
    
    def acquire_lock(
        self,
        lock_name: str,
        timeout: int = 10,
        blocking_timeout: int = 5
    ) -> Optional[Any]:
        """Acquire distributed lock."""
        try:
            return self.redis.lock(
                name=f"lock:{lock_name}",
                timeout=timeout,
                blocking_timeout=blocking_timeout
            )
            
        except Exception as e:
            logger.error(f"Lock acquisition failed: {e}")
            return None
    
    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache keys matching pattern."""
        try:
            keys = self.redis.keys(pattern)
            if keys:
                return self.redis.delete(*keys)
            return 0
            
        except Exception as e:
            logger.error(f"Pattern invalidation failed: {e}")
            return 0
    
    def get_hash_field(
        self,
        hash_key: str,
        field: str,
        default: Any = None
    ) -> Optional[Any]:
        """Get field from hash."""
        try:
            data = self.redis.hget(hash_key, field)
            return json.loads(data) if data else default
            
        except Exception as e:
            logger.error(f"Hash field get failed: {e}")
            return default
    
    def set_hash_field(
        self,
        hash_key: str,
        field: str,
        value: Any
    ) -> bool:
        """Set field in hash."""
        try:
            return bool(
                self.redis.hset(
                    hash_key,
                    field,
                    json.dumps(value)
                )
            )
            
        except Exception as e:
            logger.error(f"Hash field set failed: {e}")
            return False
    
    def close(self):
        """Close cache connection."""
        try:
            self.redis.close()
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}") 