"""
Data caching module.
"""

import logging
from typing import Dict, Any, Optional
import pandas as pd
import json
from datetime import datetime, timedelta
import hashlib

from ..core.database import db_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCache:
    """Data caching system."""
    
    def __init__(self):
        """Initialize cache."""
        self.redis = db_manager.redis
        self.default_ttl = 3600  # 1 hour
    
    def _generate_key(self, source: str, query: Dict[str, Any]) -> str:
        """Generate cache key."""
        query_str = json.dumps(query, sort_keys=True)
        key_str = f"{source}:{query_str}"
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, source: str, query: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Get data from cache."""
        try:
            key = self._generate_key(source, query)
            data = self.redis.get(key)
            
            if data:
                # Deserialize data
                df_dict = json.loads(data)
                return pd.DataFrame.from_dict(df_dict)
            
            return None
            
        except Exception as e:
            logger.error(f"Cache retrieval failed: {e}")
            return None
    
    def set(
        self,
        source: str,
        query: Dict[str, Any],
        data: pd.DataFrame,
        ttl: Optional[int] = None
    ) -> bool:
        """Set data in cache."""
        try:
            key = self._generate_key(source, query)
            
            # Serialize data
            df_dict = data.to_dict()
            data_str = json.dumps(df_dict)
            
            # Set with expiration
            self.redis.setex(
                key,
                ttl or self.default_ttl,
                data_str
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Cache storage failed: {e}")
            return False
    
    def invalidate(self, source: str, query: Dict[str, Any]) -> bool:
        """Invalidate cached data."""
        try:
            key = self._generate_key(source, query)
            self.redis.delete(key)
            return True
            
        except Exception as e:
            logger.error(f"Cache invalidation failed: {e}")
            return False
    
    def clear_all(self) -> bool:
        """Clear all cached data."""
        try:
            self.redis.flushdb()
            return True
            
        except Exception as e:
            logger.error(f"Cache clear failed: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            info = self.redis.info()
            return {
                'used_memory': info['used_memory_human'],
                'hits': info['keyspace_hits'],
                'misses': info['keyspace_misses'],
                'keys': info['db0']['keys']
            }
            
        except Exception as e:
            logger.error(f"Stats retrieval failed: {e}")
            return {}

# Global cache instance
data_cache = DataCache() 