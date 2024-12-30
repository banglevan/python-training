"""
Feature engineering exercises.
"""

from typing import Dict, Any, Optional, List, Union
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
import redis
import json
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureExtractor:
    """Feature extraction management."""
    
    def __init__(self):
        """Initialize extractor."""
        self.scalers = {}
        self.encoders = {}
        self.vectorizers = {}
    
    def extract_numeric_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        scale: bool = True
    ) -> pd.DataFrame:
        """
        Extract numeric features.
        
        Args:
            data: Input DataFrame
            columns: Columns to process
            scale: Whether to scale features
        
        Returns:
            Processed DataFrame
        """
        try:
            result = data.copy()
            
            for col in columns:
                # Handle missing values
                result[col] = result[col].fillna(result[col].mean())
                
                # Scale if needed
                if scale:
                    if col not in self.scalers:
                        self.scalers[col] = StandardScaler()
                        result[col] = self.scalers[col].fit_transform(
                            result[[col]]
                        )
                    else:
                        result[col] = self.scalers[col].transform(
                            result[[col]]
                        )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract numeric features: {e}")
            raise
    
    def extract_categorical_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        max_categories: int = 100
    ) -> pd.DataFrame:
        """
        Extract categorical features.
        
        Args:
            data: Input DataFrame
            columns: Columns to process
            max_categories: Maximum categories per column
        
        Returns:
            Processed DataFrame
        """
        try:
            result = data.copy()
            
            for col in columns:
                if col not in self.encoders:
                    # Initialize encoder
                    self.encoders[col] = OneHotEncoder(
                        sparse=False,
                        handle_unknown='ignore',
                        max_categories=max_categories
                    )
                    
                    # Fit and transform
                    encoded = self.encoders[col].fit_transform(
                        result[[col]]
                    )
                else:
                    # Transform only
                    encoded = self.encoders[col].transform(
                        result[[col]]
                    )
                
                # Add encoded columns
                feature_names = [
                    f"{col}_{cat}" for cat in 
                    self.encoders[col].categories_[0]
                ]
                
                for i, name in enumerate(feature_names):
                    result[name] = encoded[:, i]
                
                # Drop original column
                result = result.drop(col, axis=1)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract categorical features: {e}")
            raise
    
    def extract_text_features(
        self,
        data: pd.DataFrame,
        columns: List[str],
        max_features: int = 1000
    ) -> pd.DataFrame:
        """
        Extract text features.
        
        Args:
            data: Input DataFrame
            columns: Columns to process
            max_features: Maximum features per column
        
        Returns:
            Processed DataFrame
        """
        try:
            result = data.copy()
            
            for col in columns:
                if col not in self.vectorizers:
                    # Initialize vectorizer
                    self.vectorizers[col] = TfidfVectorizer(
                        max_features=max_features,
                        stop_words='english'
                    )
                    
                    # Fit and transform
                    vectorized = self.vectorizers[col].fit_transform(
                        result[col].fillna('')
                    )
                else:
                    # Transform only
                    vectorized = self.vectorizers[col].transform(
                        result[col].fillna('')
                    )
                
                # Add vectorized columns
                feature_names = [
                    f"{col}_tfidf_{i}" for i in range(vectorized.shape[1])
                ]
                
                for i, name in enumerate(feature_names):
                    result[name] = vectorized[:, i].toarray().flatten()
                
                # Drop original column
                result = result.drop(col, axis=1)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract text features: {e}")
            raise

class FeatureStore:
    """Feature storage management."""
    
    def __init__(
        self,
        redis_host: str = 'localhost',
        redis_port: int = 6379
    ):
        """Initialize store."""
        self.redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.namespace = 'features:'
    
    def store_features(
        self,
        features: Dict[str, Any],
        entity_id: str,
        ttl: Optional[int] = None
    ) -> None:
        """
        Store features.
        
        Args:
            features: Feature dictionary
            entity_id: Entity ID
            ttl: Time to live in seconds
        """
        try:
            # Serialize features
            key = f"{self.namespace}{entity_id}"
            value = json.dumps(features)
            
            # Store in Redis
            self.redis_client.set(key, value)
            
            # Set TTL if provided
            if ttl:
                self.redis_client.expire(key, ttl)
            
            logger.info(f"Stored features for {entity_id}")
            
        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            raise
    
    def get_features(
        self,
        entity_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get features.
        
        Args:
            entity_id: Entity ID
        
        Returns:
            Feature dictionary
        """
        try:
            # Get from Redis
            key = f"{self.namespace}{entity_id}"
            value = self.redis_client.get(key)
            
            if value:
                return json.loads(value)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            raise
    
    def delete_features(
        self,
        entity_id: str
    ) -> None:
        """
        Delete features.
        
        Args:
            entity_id: Entity ID
        """
        try:
            key = f"{self.namespace}{entity_id}"
            self.redis_client.delete(key)
            
            logger.info(f"Deleted features for {entity_id}")
            
        except Exception as e:
            logger.error(f"Failed to delete features: {e}")
            raise

class OnlineServing:
    """Online feature serving."""
    
    def __init__(
        self,
        feature_store: FeatureStore,
        model_path: Optional[str] = None
    ):
        """Initialize serving."""
        self.feature_store = feature_store
        self.model = None
        if model_path:
            self.load_model(model_path)
    
    def load_model(
        self,
        model_path: str
    ) -> None:
        """
        Load model.
        
        Args:
            model_path: Path to model
        """
        try:
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)
            
            logger.info("Loaded model successfully")
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
    
    def serve_features(
        self,
        entity_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Serve features.
        
        Args:
            entity_id: Entity ID
        
        Returns:
            Feature dictionary with predictions
        """
        try:
            # Get features
            features = self.feature_store.get_features(entity_id)
            
            if not features:
                return None
            
            # Add predictions if model exists
            if self.model:
                features['prediction'] = self.model.predict(
                    pd.DataFrame([features])
                )[0]
            
            return features
            
        except Exception as e:
            logger.error(f"Failed to serve features: {e}")
            raise 