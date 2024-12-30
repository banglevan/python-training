"""
State management implementation.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StateManager:
    """Manages entity state across sources."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize state manager."""
        self.config = config
        self.db_config = config['database']
        self.cache_config = config.get('cache', {})
        
        # Initialize cache if configured
        self.cache = None
        if self.cache_config:
            self.cache = redis.Redis(**self.cache_config)
    
    def get_state(
        self,
        entity_id: str,
        source_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get current entity state.
        
        Args:
            entity_id: Entity identifier
            source_type: Source system type
        """
        try:
            # Check cache first
            if self.cache:
                cached = self._get_cached_state(entity_id, source_type)
                if cached:
                    return cached
            
            # Get from database
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT entity_id, source_type, version,
                               data, checksum, updated_at
                        FROM entity_states
                        WHERE entity_id = %s
                          AND source_type = %s
                    """, (entity_id, source_type))
                    
                    result = cur.fetchone()
                    if result:
                        state = dict(result)
                        
                        # Cache result
                        if self.cache:
                            self._cache_state(entity_id, source_type, state)
                        
                        return state
            
            return None
            
        except Exception as e:
            logger.error(f"State retrieval failed: {e}")
            return None
    
    def update_state(
        self,
        entity_id: str,
        source_type: str,
        data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Update entity state.
        
        Args:
            entity_id: Entity identifier
            source_type: Source system type
            data: New entity data
        """
        try:
            checksum = self._generate_checksum(data)
            
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        INSERT INTO entity_states
                        (entity_id, source_type, version, data,
                         checksum, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (entity_id, source_type)
                        DO UPDATE SET
                            version = entity_states.version + 1,
                            data = EXCLUDED.data,
                            checksum = EXCLUDED.checksum,
                            updated_at = EXCLUDED.updated_at
                        RETURNING entity_id, source_type, version,
                                 data, checksum, updated_at
                    """, (
                        entity_id,
                        source_type,
                        1,  # Initial version
                        json.dumps(data),
                        checksum,
                        datetime.now(),
                        datetime.now()
                    ))
                    
                    result = cur.fetchone()
                    if result:
                        state = dict(result)
                        
                        # Update cache
                        if self.cache:
                            self._cache_state(entity_id, source_type, state)
                        
                        return state
                    
                conn.commit()
            
            return None
            
        except Exception as e:
            logger.error(f"State update failed: {e}")
            return None
    
    def delete_state(
        self,
        entity_id: str,
        source_type: str
    ) -> bool:
        """
        Delete entity state.
        
        Args:
            entity_id: Entity identifier
            source_type: Source system type
        """
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM entity_states
                        WHERE entity_id = %s
                          AND source_type = %s
                    """, (entity_id, source_type))
                conn.commit()
            
            # Clear cache
            if self.cache:
                self._clear_cached_state(entity_id, source_type)
            
            return True
            
        except Exception as e:
            logger.error(f"State deletion failed: {e}")
            return False
    
    def _generate_checksum(self, data: Dict[str, Any]) -> str:
        """Generate checksum for data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    def _get_cached_state(
        self,
        entity_id: str,
        source_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get state from cache."""
        try:
            if self.cache:
                key = f"state:{source_type}:{entity_id}"
                data = self.cache.get(key)
                return json.loads(data) if data else None
            return None
            
        except Exception as e:
            logger.error(f"Cache retrieval failed: {e}")
            return None
    
    def _cache_state(
        self,
        entity_id: str,
        source_type: str,
        state: Dict[str, Any]
    ):
        """Cache entity state."""
        try:
            if self.cache:
                key = f"state:{source_type}:{entity_id}"
                self.cache.setex(
                    key,
                    self.cache_config.get('ttl', 3600),
                    json.dumps(state)
                )
        except Exception as e:
            logger.error(f"State caching failed: {e}")
    
    def _clear_cached_state(
        self,
        entity_id: str,
        source_type: str
    ):
        """Clear cached state."""
        try:
            if self.cache:
                key = f"state:{source_type}:{entity_id}"
                self.cache.delete(key)
        except Exception as e:
            logger.error(f"Cache clearing failed: {e}")
    
    def close(self):
        """Close state manager."""
        try:
            if self.cache:
                self.cache.close()
        except Exception as e:
            logger.error(f"State manager cleanup failed: {e}") 