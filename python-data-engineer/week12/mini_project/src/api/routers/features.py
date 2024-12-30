"""
Feature engineering router.
"""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from datetime import datetime

from src.security.auth_manager import AuthManager
from src.security.access_control import AccessControl
from src.processing.feature_engineering import FeatureEngineering
from src.storage.feature_store import FeatureStore

router = APIRouter(
    prefix="/features",
    tags=["features"]
)

# Models
class FeatureConfig(BaseModel):
    name: str
    config: Dict[str, Any]

class CreateFeaturesRequest(BaseModel):
    data_source: str
    configs: List[str]
    namespace: str
    entity_col: str

class FeatureBatchRequest(BaseModel):
    namespace: str
    entity_ids: List[str]

@router.post("/configs")
async def add_feature_config(
    request: FeatureConfig,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Add feature configuration."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "features",
            "configure"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get feature engineering
        feature_eng = FeatureEngineering(spark, feature_store)
        
        # Add config
        feature_eng.add_feature_config(
            request.name,
            request.config
        )
        
        return {
            "message": f"Feature config added: {request.name}"
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.post("/create")
async def create_features(
    request: CreateFeaturesRequest,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Create and store features."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "features",
            "create"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get components
        feature_eng = FeatureEngineering(spark, feature_store)
        
        # Load data
        data = spark.read.table(request.data_source)
        
        # Create features
        features = feature_eng.create_features(
            data,
            request.configs
        )
        
        # Store features
        feature_eng.store_features(
            features,
            request.namespace,
            request.entity_col
        )
        
        return {
            "message": "Features created and stored successfully",
            "namespace": request.namespace,
            "num_entities": features.count()
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.get("/batch")
async def get_batch_features(
    request: FeatureBatchRequest,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get features for multiple entities."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "features",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get feature store
        store = FeatureStore()
        
        # Get features
        features = store.get_multi_features(
            request.namespace,
            request.entity_ids
        )
        
        return features
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        ) 