"""
API routes module.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..core.security import security_manager, oauth2_scheme
from ..core.database import db_manager
from ..data.sources import SourceFactory
from ..data.cache import data_cache
from ..viz.dashboards import dashboard_manager
from ..viz.themes import theme_manager
from . import schemas

# Create routers
api_router = APIRouter()
auth_router = APIRouter(prefix="/auth", tags=["auth"])
data_router = APIRouter(prefix="/data", tags=["data"])
viz_router = APIRouter(prefix="/viz", tags=["visualization"])

# Authentication routes
@auth_router.post("/login", response_model=schemas.Token)
async def login(user: schemas.UserCreate):
    """Login user."""
    try:
        with db_manager.get_db() as db:
            result = db.execute(
                "SELECT * FROM users WHERE username = %s",
                (user.username,)
            ).fetchone()
            
            if not result or not security_manager.verify_password(
                user.password,
                result['password_hash']
            ):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid credentials"
                )
            
            access_token = security_manager.create_access_token(
                data={"sub": user.username, "role": result['role']}
            )
            
            return {"access_token": access_token, "token_type": "bearer"}
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@auth_router.post("/users", response_model=schemas.User)
async def create_user(
    user: schemas.UserCreate,
    current_user: Dict = Depends(security_manager.get_current_user)
):
    """Create new user."""
    if not security_manager.check_permissions("admin", current_user["role"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    
    try:
        with db_manager.get_db() as db:
            password_hash = security_manager.get_password_hash(user.password)
            
            result = db.execute(
                """
                INSERT INTO users (username, email, password_hash, role)
                VALUES (%s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (user.username, user.email, password_hash, user.role)
            ).fetchone()
            
            db.commit()
            
            return {**user.dict(), "id": result['id'], "created_at": result['created_at']}
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Data routes
@data_router.post("/sources")
async def create_data_source(
    source: schemas.DataSourceConfig,
    current_user: Dict = Depends(security_manager.get_current_user)
):
    """Create data source."""
    if not security_manager.check_permissions("editor", current_user["role"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )
    
    try:
        # Validate source configuration
        source_instance = SourceFactory.create_source(source.dict())
        
        # Store configuration
        with db_manager.get_db() as db:
            result = db.execute(
                """
                INSERT INTO data_sources (name, type, config)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (source.name, source.type, source.config)
            ).fetchone()
            
            db.commit()
            
            return {"id": result['id']}
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Visualization routes
@viz_router.post("/dashboards")
async def create_dashboard(
    dashboard: schemas.DashboardCreate,
    current_user: Dict = Depends(security_manager.get_current_user)
):
    """Create dashboard."""
    try:
        dashboard_id = dashboard_manager.create_dashboard(
            dashboard.dict(),
            current_user
        )
        
        if not dashboard_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Dashboard creation failed"
            )
        
        return {"id": dashboard_id}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@viz_router.get("/dashboards/{dashboard_id}")
async def get_dashboard(
    dashboard_id: str,
    current_user: Dict = Depends(security_manager.get_current_user)
):
    """Get dashboard by ID."""
    try:
        dashboard = dashboard_manager.get_dashboard(dashboard_id, current_user)
        
        if not dashboard:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dashboard not found"
            )
        
        return dashboard
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Include all routers
api_router.include_router(auth_router)
api_router.include_router(data_router)
api_router.include_router(viz_router) 