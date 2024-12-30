"""
Feature engineering pipeline.
"""

from typing import Dict, Any, Optional, List, Union
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureEngineering:
    """Feature engineering pipeline."""
    
    def __init__(
        self,
        spark: SparkSession,
        feature_store: Any
    ):
        """Initialize pipeline."""
        self.spark = spark
        self.feature_store = feature_store
        self.transformers = {}
        self.feature_configs = {}
    
    def add_feature_config(
        self,
        name: str,
        config: Dict[str, Any]
    ) -> None:
        """
        Add feature configuration.
        
        Args:
            name: Feature name
            config: Configuration dictionary
        """
        try:
            self.feature_configs[name] = config
            logger.info(f"Added feature config: {name}")
            
        except Exception as e:
            logger.error(f"Failed to add feature config: {e}")
            raise
    
    def create_features(
        self,
        data: Any,
        configs: Optional[List[str]] = None
    ) -> Any:
        """
        Create features.
        
        Args:
            data: Input DataFrame
            configs: Feature configurations to use
        
        Returns:
            DataFrame with features
        """
        try:
            result = data
            
            # Use specified configs or all configs
            configs = configs or list(self.feature_configs.keys())
            
            for name in configs:
                if name not in self.feature_configs:
                    raise ValueError(f"Invalid feature config: {name}")
                
                config = self.feature_configs[name]
                result = self._apply_feature_config(result, name, config)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to create features: {e}")
            raise
    
    def store_features(
        self,
        features: Any,
        namespace: str,
        entity_col: str
    ) -> None:
        """
        Store features.
        
        Args:
            features: Feature DataFrame
            namespace: Feature namespace
            entity_col: Entity ID column
        """
        try:
            # Convert to pandas for processing
            pdf = features.toPandas()
            
            # Store features by entity
            for _, row in pdf.iterrows():
                entity_id = row[entity_col]
                feature_dict = row.drop(entity_col).to_dict()
                
                self.feature_store.store_features(
                    namespace,
                    entity_id,
                    feature_dict
                )
            
            logger.info(f"Stored features in namespace: {namespace}")
            
        except Exception as e:
            logger.error(f"Failed to store features: {e}")
            raise
    
    def _apply_feature_config(
        self,
        data: Any,
        name: str,
        config: Dict[str, Any]
    ) -> Any:
        """Apply feature configuration."""
        feature_type = config.get('type')
        
        if feature_type == 'numeric':
            return self._create_numeric_features(
                data,
                config.get('columns', []),
                config.get('operations', [])
            )
            
        elif feature_type == 'categorical':
            return self._create_categorical_features(
                data,
                config.get('columns', []),
                config.get('max_categories', 100)
            )
            
        elif feature_type == 'temporal':
            return self._create_temporal_features(
                data,
                config.get('column'),
                config.get('features', [])
            )
            
        else:
            raise ValueError(f"Invalid feature type: {feature_type}")
    
    def _create_numeric_features(
        self,
        data: Any,
        columns: List[str],
        operations: List[str]
    ) -> Any:
        """Create numeric features."""
        result = data
        
        for col in columns:
            for op in operations:
                if op == 'standardize':
                    result = result.withColumn(
                        f"{col}_scaled",
                        (col(col) - mean(col)) / stddev(col)
                    )
                elif op == 'log':
                    result = result.withColumn(
                        f"{col}_log",
                        log(col(col))
                    )
        
        return result
    
    def _create_categorical_features(
        self,
        data: Any,
        columns: List[str],
        max_categories: int
    ) -> Any:
        """Create categorical features."""
        result = data
        
        for col in columns:
            # Get distinct values
            distinct = result.select(col).distinct()
            
            if distinct.count() > max_categories:
                # Too many categories - use hashing
                result = result.withColumn(
                    f"{col}_hash",
                    hash(col(col)) % max_categories
                )
            else:
                # One-hot encoding
                distinct_values = [
                    r[col] for r in distinct.collect()
                ]
                
                for value in distinct_values:
                    result = result.withColumn(
                        f"{col}_{value}",
                        when(col(col) == value, 1).otherwise(0)
                    )
        
        return result
    
    def _create_temporal_features(
        self,
        data: Any,
        column: str,
        features: List[str]
    ) -> Any:
        """Create temporal features."""
        result = data
        
        for feature in features:
            if feature == 'hour':
                result = result.withColumn(
                    f"{column}_hour",
                    hour(col(column))
                )
            elif feature == 'day':
                result = result.withColumn(
                    f"{column}_day",
                    dayofmonth(col(column))
                )
            elif feature == 'month':
                result = result.withColumn(
                    f"{column}_month",
                    month(col(column))
                )
            elif feature == 'year':
                result = result.withColumn(
                    f"{column}_year",
                    year(col(column))
                )
            elif feature == 'dayofweek':
                result = result.withColumn(
                    f"{column}_dayofweek",
                    dayofweek(col(column))
                )
        
        return result 