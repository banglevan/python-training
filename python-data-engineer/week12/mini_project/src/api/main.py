"""
Main API application.
"""

from typing import Dict, Any, Optional, List
import logging
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from datetime import datetime
import uvicorn

from src.security.auth_manager import AuthManager
from src.security.access_control import AccessControl
from src.storage.delta_manager import DeltaManager
from src.storage.feature_store import FeatureStore
from src.processing.query_optimizer import QueryOptimizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Data Platform API",
    description="API for optimized data platform",
    version="1.0.0"
)

# Initialize components
auth_manager = AuthManager(secret_key="your-secret-key")
access_control = AccessControl()

# Models
class UserCreate(BaseModel):
    username: str
    password: str
    roles: List[str] = ["user"]

class LoginRequest(BaseModel):
    username: str
    password: str

class QueryRequest(BaseModel):
    query: str
    collect_stats: bool = True

class FeatureRequest(BaseModel):
    entity_id: str
    namespace: str

@app.post("/auth/register")
async def register_user(user: UserCreate):
    """Register new user."""
    try:
        auth_manager.create_user(
            user.username,
            user.password,
            user.roles
        )
        return {"message": "User registered successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@app.post("/auth/login")
async def login(request: LoginRequest):
    """Login user."""
    try:
        token = auth_manager.authenticate(
            request.username,
            request.password
        )
        return {"access_token": token}
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials"
        )

@app.post("/query/optimize")
async def optimize_query(
    request: QueryRequest,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Optimize SQL query."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "query",
            "optimize"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get optimizer
        optimizer = QueryOptimizer(spark)
        
        # Optimize query
        result = optimizer.optimize_query(
            request.query,
            request.collect_stats
        )
        
        return result
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@app.get("/features/{namespace}/{entity_id}")
async def get_features(
    namespace: str,
    entity_id: str,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get entity features."""
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
        features = store.get_features(namespace, entity_id)
        
        if not features:
            raise HTTPException(
                status_code=404,
                detail="Features not found"
            )
        
        return features
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/metrics")
async def get_metrics(
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get platform metrics."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "metrics",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Collect metrics
        metrics = {
            "users": len(auth_manager.users),
            "policies": len(access_control.policies),
            "timestamp": datetime.now().isoformat()
        }
        
        return metrics
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 