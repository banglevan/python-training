"""
Feature store management using Redis.
"""

from typing import Dict, Any, Optional, List, Union
import logging
import redis
import json
from datetime import datetime, timedelta
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureStore:
    """Feature store management."""
    
    def __init__(
        self,
        host: str,
        port: int,
        db: int = 0,
        password: Optional[str] = None
    ):
        """Initialize store."""
        self.client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        self.namespaces = {}
    
    def create_namespace(
        self,
        name: str,
        ttl: Optional[int] = None
    ) -> None:
        """
        Create feature namespace.
        
        Args:
            name: Namespace name
            ttl: Default TTL in seconds
        """
        try:
            self.namespaces[name] = {
                'ttl': ttl,
                'created_at': datetime.now().isoformat()
            }
            logger.info(f"Created namespace: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create namespace: {e}")
            raise
    
    def store_features(
        self,
        namespace: str,
        entity_id: str,
        features: Dict[str, Any],
        ttl: Optional[int] = None
    ) -> None:
        """
        Store features.
        
        Args:
            namespace: Feature namespace
            entity_id: Entity ID
            features: Feature dictionary
            ttl: Time to live in seconds
        """
        try:
            # Validate namespace
            if namespace not in self.namespaces:
                raise ValueError(f"Invalid namespace: {namespace}")
            
            # Prepare key
            key = f"{namespace}:{entity_id}"
            
            # Convert numpy arrays to lists
            processed_features = {}
            for k, v in features.items():
                if isinstance(v, np.ndarray):
                    processed_features[k] = v.tolist()
                else:
                    processed_features[k] = v
            
            # Store features
            self.client.set(
                key,
                json.dumps(processed_features)
            )
            
            # Set TTL
            if ttl or self.namespaces[namespace]['ttl']:
                self.client.expire(
                    key,
                    ttl or self.namespaces[namespace]['ttl']
                )
            
            logger.info(f"Stored features for {key}")
            
        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            raise
    
    def get_features(
        self,
        namespace: str,
        entity_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get features.
        
        Args:
            namespace: Feature namespace
            entity_id: Entity ID
        
        Returns:
            Feature dictionary
        """
        try:
            # Validate namespace
            if namespace not in self.namespaces:
                raise ValueError(f"Invalid namespace: {namespace}")
            
            # Get features
            key = f"{namespace}:{entity_id}"
            value = self.client.get(key)
            
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            raise
    
    def get_multi_features(
        self,
        namespace: str,
        entity_ids: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get multiple features.
        
        Args:
            namespace: Feature namespace
            entity_ids: List of entity IDs
        
        Returns:
            Dictionary of features
        """
        try:
            # Validate namespace
            if namespace not in self.namespaces:
                raise ValueError(f"Invalid namespace: {namespace}")
            
            # Get keys
            keys = [f"{namespace}:{eid}" for eid in entity_ids]
            
            # Get features
            values = self.client.mget(keys)
            
            return {
                eid: json.loads(value) if value else None
                for eid, value in zip(entity_ids, values)
            }
            
        except Exception as e:
            logger.error(f"Failed to get multiple features: {e}")
            raise
    
    def delete_features(
        self,
        namespace: str,
        entity_id: str
    ) -> None:
        """
        Delete features.
        
        Args:
            namespace: Feature namespace
            entity_id: Entity ID
        """
        try:
            # Validate namespace
            if namespace not in self.namespaces:
                raise ValueError(f"Invalid namespace: {namespace}")
            
            # Delete features
            key = f"{namespace}:{entity_id}"
            self.client.delete(key)
            
            logger.info(f"Deleted features for {key}")
            
        except Exception as e:
            logger.error(f"Failed to delete features: {e}")
            raise 