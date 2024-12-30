"""
Python client for feature serving API.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import requests
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class FeatureClient:
    """Client for feature serving API."""
    
    def __init__(self, base_url: str):
        """
        Initialize client.
        
        Args:
            base_url: Base URL for feature serving API
        """
        self.base_url = base_url.rstrip('/')
    
    def get_online_features(
        self,
        entity_type: str,
        entity_ids: List[str],
        features: List[str],
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Get online feature values.
        
        Args:
            entity_type: Type of entity (e.g., 'customer', 'product')
            entity_ids: List of entity IDs
            features: List of feature names
            use_cache: Whether to use Redis cache
            
        Returns:
            Dictionary with features and metadata
        """
        try:
            response = requests.post(
                f"{self.base_url}/features/online",
                json={
                    "entity_type": entity_type,
                    "entity_ids": entity_ids,
                    "features": features,
                    "use_redis_cache": use_cache
                }
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get online features: {e}")
            raise
    
    def get_historical_features(
        self,
        entity_type: str,
        entity_ids: List[str],
        features: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get historical feature values.
        
        Args:
            entity_type: Type of entity
            entity_ids: List of entity IDs
            features: List of feature names
            start_date: Start date for historical features
            end_date: End date for historical features
            
        Returns:
            DataFrame with historical features
        """
        try:
            response = requests.post(
                f"{self.base_url}/features/historical",
                json={
                    "entity_type": entity_type,
                    "entity_ids": entity_ids,
                    "features": features,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
            )
            response.raise_for_status()
            
            data = response.json()
            return pd.DataFrame(data['features'])
            
        except Exception as e:
            logger.error(f"Failed to get historical features: {e}")
            raise
    
    def get_feature_metadata(self) -> Dict[str, Any]:
        """
        Get feature metadata.
        
        Returns:
            Dictionary with feature metadata
        """
        try:
            response = requests.get(
                f"{self.base_url}/features/metadata"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get feature metadata: {e}")
            raise
    
    def get_serving_metrics(self) -> Dict[str, Any]:
        """
        Get serving metrics.
        
        Returns:
            Dictionary with serving metrics
        """
        try:
            response = requests.get(
                f"{self.base_url}/metrics"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get serving metrics: {e}")
            raise 