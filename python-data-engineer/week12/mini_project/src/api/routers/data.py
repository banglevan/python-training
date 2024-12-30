"""
Data operations router.
"""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from datetime import datetime

from src.security.auth_manager import AuthManager
from src.security.access_control import AccessControl
from src.storage.delta_manager import DeltaManager

router = APIRouter(
    prefix="/data",
    tags=["data"]
)

# Models
class TableCreate(BaseModel):
    name: str
    schema: Dict[str, str]
    partition_by: Optional[List[str]] = None
    location: Optional[str] = None

class TableOptimize(BaseModel):
    name: str
    zorder_by: Optional[List[str]] = None

@router.post("/tables")
async def create_table(
    request: TableCreate,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Create Delta table."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "table",
            "create"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get manager
        manager = DeltaManager(spark)
        
        # Create table
        manager.create_table(
            request.name,
            request.schema,
            request.partition_by,
            request.location
        )
        
        return {"message": f"Table created: {request.name}"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.post("/tables/{name}/optimize")
async def optimize_table(
    name: str,
    request: TableOptimize,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Optimize Delta table."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "table",
            "optimize"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get manager
        manager = DeltaManager(spark)
        
        # Optimize table
        manager.optimize_table(
            name,
            request.zorder_by
        )
        
        return {"message": f"Table optimized: {name}"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.get("/tables/{name}/stats")
async def get_table_stats(
    name: str,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get table statistics."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "table",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get manager
        manager = DeltaManager(spark)
        
        # Get stats
        stats = manager.get_table_stats(name)
        
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        ) 