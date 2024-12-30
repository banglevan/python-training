"""
FastAPI application for feature serving.
"""

from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import logging
from src.data.sinks.feast import FeastSink
from src.data.sinks.redis import RedisSink
from src.monitoring.metrics import MetricsCollector

app = FastAPI(title="Feature Serving API")
logger = logging.getLogger(__name__)

# Initialize components
feast_sink = FeastSink(
    repo_path="feature_repo/",
    project="ml_feature_platform"
)
redis_sink = RedisSink(
    host="localhost",
    port=6379
)
metrics = MetricsCollector()

class FeatureRequest(BaseModel):
    """Feature request model."""
    entity_type: str
    entity_ids: List[str]
    features: List[str]
    use_redis_cache: bool = True

class HistoricalFeatureRequest(BaseModel):
    """Historical feature request model."""
    entity_type: str
    entity_ids: List[str]
    features: List[str]
    start_date: datetime
    end_date: datetime

@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

@app.post("/features/online")
def get_online_features(request: FeatureRequest) -> Dict[str, Any]:
    """Get online feature values."""
    try:
        metrics.start_operation("get_online_features")
        
        features = []
        cache_hits = 0
        
        # Try Redis cache first if enabled
        if request.use_redis_cache:
            for entity_id in request.entity_ids:
                cache_key = f"{request.entity_type}_{entity_id}"
                cached_features = redis_sink.get_features(cache_key)
                
                if cached_features:
                    features.append(cached_features)
                    cache_hits += 1
                    continue
        
        # Get remaining features from Feast
        if len(features) < len(request.entity_ids):
            entity_rows = [
                {request.entity_type: entity_id}
                for entity_id in request.entity_ids
                if not any(f.get('entity_id') == entity_id for f in features)
            ]
            
            feast_features = feast_sink.get_online_features(
                feature_refs=request.features,
                entity_rows=entity_rows
            )
            
            features.extend(feast_features)
        
        duration = metrics.end_operation("get_online_features")
        
        # Record metrics
        metrics.record_value(
            "feature_requests",
            1,
            {"type": "online"}
        )
        metrics.record_value(
            "cache_hit_rate",
            cache_hits / len(request.entity_ids)
        )
        
        return {
            "features": features,
            "metadata": {
                "cache_hits": cache_hits,
                "total_entities": len(request.entity_ids),
                "duration_ms": duration * 1000
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get online features: {e}")
        metrics.record_error("get_online_features")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.post("/features/historical")
def get_historical_features(
    request: HistoricalFeatureRequest
) -> Dict[str, Any]:
    """Get historical feature values."""
    try:
        metrics.start_operation("get_historical_features")
        
        # Create entity dataframe
        entity_df = pd.DataFrame({
            request.entity_type: request.entity_ids,
            'event_timestamp': [request.end_date] * len(request.entity_ids)
        })
        
        # Get historical features
        features_df = feast_sink.get_historical_features(
            feature_refs=request.features,
            entity_df=entity_df
        )
        
        duration = metrics.end_operation("get_historical_features")
        
        # Record metrics
        metrics.record_value(
            "feature_requests",
            1,
            {"type": "historical"}
        )
        
        return {
            "features": features_df.to_dict(orient='records'),
            "metadata": {
                "total_entities": len(request.entity_ids),
                "duration_ms": duration * 1000
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get historical features: {e}")
        metrics.record_error("get_historical_features")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/features/metadata")
def get_feature_metadata() -> Dict[str, Any]:
    """Get feature metadata."""
    try:
        # Get feature views from Feast
        feature_views = feast_sink.store.list_feature_views()
        
        metadata = {}
        for view in feature_views:
            metadata[view.name] = {
                "features": [f.name for f in view.features],
                "entities": view.entities,
                "ttl": str(view.ttl),
                "online": view.online
            }
        
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to get feature metadata: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/metrics")
def get_serving_metrics() -> Dict[str, Any]:
    """Get serving metrics."""
    try:
        return metrics.get_metrics()
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        ) 