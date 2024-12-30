"""
Platform monitoring router.
"""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import HTTPBearer
from pydantic import BaseModel
from datetime import datetime, timedelta
import psutil
import json

router = APIRouter(
    prefix="/monitoring",
    tags=["monitoring"]
)

class SystemMetrics(BaseModel):
    cpu_percent: float
    memory_percent: float
    disk_usage: Dict[str, float]
    timestamp: str

class StorageMetrics(BaseModel):
    total_tables: int
    total_size: int
    table_metrics: Dict[str, Dict[str, Any]]
    timestamp: str

@router.get("/system", response_model=SystemMetrics)
async def get_system_metrics(
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get system metrics."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "monitoring",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Collect metrics
        metrics = {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage": {
                path.mountpoint: psutil.disk_usage(path.mountpoint).percent
                for path in psutil.disk_partitions()
            },
            "timestamp": datetime.now().isoformat()
        }
        
        return metrics
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.get("/storage", response_model=StorageMetrics)
async def get_storage_metrics(
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get storage metrics."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "monitoring",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Get manager
        manager = DeltaManager(spark)
        
        # Collect metrics
        table_metrics = {}
        total_size = 0
        
        for table in manager.tables:
            stats = manager.get_table_stats(table)
            table_metrics[table] = stats
            total_size += stats['size_bytes']
        
        metrics = {
            "total_tables": len(manager.tables),
            "total_size": total_size,
            "table_metrics": table_metrics,
            "timestamp": datetime.now().isoformat()
        }
        
        return metrics
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.get("/audit-log")
async def get_audit_log(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get audit log entries."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "audit",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Set default time range
        if not start_time:
            start_time = datetime.now() - timedelta(days=1)
        if not end_time:
            end_time = datetime.now()
        
        # Get audit logs
        logs = spark.sql(f"""
            SELECT *
            FROM audit_log
            WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
            ORDER BY timestamp DESC
        """).collect()
        
        return [json.loads(log.to_json()) for log in logs]
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )

@router.get("/alerts")
async def get_alerts(
    severity: Optional[str] = None,
    token: HTTPAuthorizationCredentials = Security(HTTPBearer())
):
    """Get system alerts."""
    try:
        # Verify token
        payload = auth_manager.verify_token(token)
        
        # Check permission
        if not access_control.check_access(
            payload["roles"],
            "alerts",
            "read"
        ):
            raise HTTPException(
                status_code=403,
                detail="Permission denied"
            )
        
        # Build query
        query = "SELECT * FROM alerts"
        if severity:
            query += f" WHERE severity = '{severity}'"
        query += " ORDER BY timestamp DESC"
        
        # Get alerts
        alerts = spark.sql(query).collect()
        
        return [json.loads(alert.to_json()) for alert in alerts]
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        ) 