"""
Storage Module
----------

Handles data storage:
1. PostgreSQL for persistence
2. Redis for caching
"""

import logging
from typing import Dict, Any
from datetime import datetime
import json
from sqlalchemy import create_engine
import redis
from prometheus_client import Counter, Histogram

from .utils import retry_with_logging

logger = logging.getLogger(__name__)

# Metrics
STORAGE_OPERATIONS = Counter(
    'storage_operations_total',
    'Number of storage operations',
    ['type', 'status']
)
STORAGE_TIME = Histogram(
    'storage_operation_seconds',
    'Time spent on storage operations',
    ['type']
)

class DataStorage:
    """Data storage implementation."""
    
    def __init__(
        self,
        db_url: str,
        redis_host: str,
        redis_port: int
    ):
        """Initialize storage."""
        self.db_engine = create_engine(db_url)
        self.redis = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
    
    @retry_with_logging
    def store_measurement(
        self,
        data: Dict[str, Any]
    ):
        """Store measurement in PostgreSQL."""
        with STORAGE_TIME.labels(type='db').time():
            try:
                with self.db_engine.connect() as conn:
                    conn.execute("""
                        INSERT INTO measurements (
                            sensor_id,
                            metric,
                            value,
                            timestamp
                        ) VALUES (
                            %(sensor_id)s,
                            %(metric)s,
                            %(value)s,
                            %(timestamp)s
                        )
                    """, data)
                    
                STORAGE_OPERATIONS.labels(
                    type='db',
                    status='success'
                ).inc()
                
            except Exception as e:
                logger.error(f"Database storage failed: {e}")
                STORAGE_OPERATIONS.labels(
                    type='db',
                    status='error'
                ).inc()
                raise
    
    @retry_with_logging
    def cache_data(
        self,
        key: str,
        data: Dict[str, Any],
        expiry: int = 3600
    ):
        """Cache data in Redis."""
        with STORAGE_TIME.labels(type='cache').time():
            try:
                self.redis.setex(
                    key,
                    expiry,
                    json.dumps(data)
                )
                
                STORAGE_OPERATIONS.labels(
                    type='cache',
                    status='success'
                ).inc()
                
            except Exception as e:
                logger.error(f"Cache storage failed: {e}")
                STORAGE_OPERATIONS.labels(
                    type='cache',
                    status='error'
                ).inc()
                raise
    
    def close(self):
        """Close storage connections."""
        try:
            self.redis.close()
            self.db_engine.dispose()
            logger.info("Storage connections closed")
            
        except Exception as e:
            logger.error(f"Storage cleanup failed: {e}") 