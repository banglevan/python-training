"""
Feast integration for feature storage and serving.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from feast import FeatureStore, Entity, Feature, FeatureView, ValueType
from feast.infra.offline_stores.file_source import FileSource
from feast.data_source import PushSource
import logging
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

class FeastSink:
    """Feast integration for feature management."""
    
    def __init__(
        self,
        repo_path: str,
        project: str = "ml_feature_platform"
    ):
        """
        Initialize Feast sink.
        
        Args:
            repo_path: Path to Feast repository
            project: Feast project name
        """
        self.store = FeatureStore(repo_path=repo_path)
        self.project = project
        self.metrics = MetricsCollector()
    
    def create_feature_view(
        self,
        name: str,
        features: List[Feature],
        entities: List[Entity],
        source: FileSource,
        ttl: timedelta = timedelta(days=1)
    ) -> FeatureView:
        """
        Create or update feature view.
        
        Args:
            name: Feature view name
            features: List of features
            entities: List of entities
            source: Data source
            ttl: Time-to-live for features
            
        Returns:
            Created feature view
        """
        try:
            self.metrics.start_operation("create_feature_view")
            
            feature_view = FeatureView(
                name=name,
                entities=[entity.name for entity in entities],
                ttl=ttl,
                features=features,
                online=True,
                source=source,
                tags={"project": self.project}
            )
            
            # Apply feature view
            self.store.apply([feature_view] + entities)
            
            duration = self.metrics.end_operation("create_feature_view")
            logger.info(
                f"Created feature view {name} in {duration:.2f}s"
            )
            
            return feature_view
            
        except Exception as e:
            logger.error(f"Failed to create feature view: {e}")
            self.metrics.record_error("create_feature_view")
            raise
    
    def materialize_features(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> None:
        """
        Materialize features for given time range.
        
        Args:
            start_date: Start date for materialization
            end_date: End date for materialization
        """
        try:
            self.metrics.start_operation("materialize_features")
            
            self.store.materialize(
                start_date=start_date,
                end_date=end_date
            )
            
            duration = self.metrics.end_operation("materialize_features")
            logger.info(
                f"Materialized features from {start_date} to {end_date} "
                f"in {duration:.2f}s"
            )
            
        except Exception as e:
            logger.error(f"Failed to materialize features: {e}")
            self.metrics.record_error("materialize_features")
            raise
    
    def get_online_features(
        self,
        feature_refs: List[str],
        entity_rows: List[Dict[str, Any]]
    ) -> Dict[str, List[Any]]:
        """
        Get online feature values.
        
        Args:
            feature_refs: List of feature references
            entity_rows: List of entity keys
            
        Returns:
            Dictionary of feature values
        """
        try:
            self.metrics.start_operation("get_online_features")
            
            response = self.store.get_online_features(
                features=feature_refs,
                entity_rows=entity_rows
            ).to_dict()
            
            duration = self.metrics.end_operation("get_online_features")
            logger.debug(
                f"Retrieved {len(feature_refs)} features for "
                f"{len(entity_rows)} entities in {duration:.2f}s"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to get online features: {e}")
            self.metrics.record_error("get_online_features")
            raise
    
    def push_features_to_online_store(
        self,
        feature_view_name: str,
        df: pd.DataFrame,
        to_proto_values: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Push features to online store.
        
        Args:
            feature_view_name: Name of feature view
            df: DataFrame with features
            to_proto_values: Optional value type mapping
        """
        try:
            self.metrics.start_operation("push_features")
            
            self.store.push(
                feature_view_name=feature_view_name,
                df=df,
                to_proto_values=to_proto_values
            )
            
            duration = self.metrics.end_operation("push_features")
            logger.info(
                f"Pushed {len(df)} rows to feature view {feature_view_name} "
                f"in {duration:.2f}s"
            )
            
        except Exception as e:
            logger.error(f"Failed to push features: {e}")
            self.metrics.record_error("push_features")
            raise
    
    def get_historical_features(
        self,
        feature_refs: List[str],
        entity_df: pd.DataFrame,
        full_feature_names: bool = True
    ) -> pd.DataFrame:
        """
        Get historical feature values.
        
        Args:
            feature_refs: List of feature references
            entity_df: Entity DataFrame
            full_feature_names: Use full feature names
            
        Returns:
            DataFrame with historical features
        """
        try:
            self.metrics.start_operation("get_historical_features")
            
            job = self.store.get_historical_features(
                features=feature_refs,
                entity_df=entity_df,
                full_feature_names=full_feature_names
            )
            
            df = job.to_df()
            
            duration = self.metrics.end_operation("get_historical_features")
            logger.info(
                f"Retrieved historical features for {len(df)} rows "
                f"in {duration:.2f}s"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get historical features: {e}")
            self.metrics.record_error("get_historical_features")
            raise 