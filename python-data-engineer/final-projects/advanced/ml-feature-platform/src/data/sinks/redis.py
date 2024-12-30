"""
Redis sink for online feature serving.
"""

from typing import Dict, Any, List
import redis
import json
import logging
from datetime import datetime
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

class RedisSink:
    """Redis sink for online feature serving."""
    
    def __init__(
        self,
        host: str,
        port: int,
        db: int = 0,
        prefix: str = "features"
    ):
        """Initialize Redis sink."""
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
        self.prefix = prefix
        self.metrics = MetricsCollector()
    
    def store_features(
        self,
        entity_key: str,
        features: Dict[str, Any],
        ttl: int = 86400  # 24 hours
    ) -> None:
        """
        Store features in Redis.
        
        Args:
            entity_key: Entity identifier
            features: Feature dictionary
            ttl: Time-to-live in seconds
        """
        try:
            self.metrics.start_operation("store_features")
            
            # Add timestamp
            features['_timestamp'] = datetime.now().isoformat()
            
            # Store features
            key = f"{self.prefix}:{entity_key}"
            self.client.setex(
                key,
                ttl,
                json.dumps(features)
            )
            
            duration = self.metrics.end_operation("store_features")
            logger.debug(
                f"Stored features for {entity_key} in {duration:.2f}s"
            )
            
            # Record metrics
            self.metrics.record_value("features_stored", 1)
            
        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            self.metrics.record_error("store_features")
            raise
    
    def get_features(
        self,
        entity_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get features from Redis.
        
        Args:
            entity_key: Entity identifier
            
        Returns:
            Feature dictionary if exists, None otherwise
        """
        try:
            self.metrics.start_operation("get_features")
            
            # Get features
            key = f"{self.prefix}:{entity_key}"
            data = self.client.get(key)
            
            if data:
                features = json.loads(data)
                duration = self.metrics.end_operation("get_features")
                logger.debug(
                    f"Retrieved features for {entity_key} in {duration:.2f}s"
                )
                return features
            
            duration = self.metrics.end_operation("get_features")
            logger.debug(
                f"No features found for {entity_key} in {duration:.2f}s"
            )
            return None
            
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            self.metrics.record_error("get_features")
            raise
    
    def delete_features(self, entity_key: str) -> None:
        """
        Delete features from Redis.
        
        Args:
            entity_key: Entity identifier
        """
        try:
            key = f"{self.prefix}:{entity_key}"
            self.client.delete(key)
            logger.debug(f"Deleted features for {entity_key}")
            
        except Exception as e:
            logger.error(f"Failed to delete features: {e}")
            self.metrics.record_error("delete_features")
            raise 