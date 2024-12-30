"""
Change Tracking Implementation
------------------------

Practice with:
1. Version control
2. Audit logging
3. History management
"""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import json
from dataclasses import dataclass
from enum import Enum
import psycopg2
from psycopg2.extras import DictCursor, Json
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OperationType(Enum):
    """Types of operations."""
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'
    READ = 'READ'

@dataclass
class AuditLog:
    """Audit log entry."""
    id: int
    user_id: str
    operation: OperationType
    table_name: str
    record_id: Union[int, str]
    changes: Optional[Dict[str, Any]]
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None

class ChangeTracking:
    """Change tracking implementation."""
    
    def __init__(
        self,
        db_config: Dict[str, Any],
        redis_config: Dict[str, Any]
    ):
        """Initialize change tracker."""
        self.db_config = db_config
        self.redis = redis.Redis(**redis_config)
        
        # Create tables
        self._init_tables()
    
    def _init_tables(self):
        """Initialize required tables."""
        with self._get_db_conn() as conn:
            with conn.cursor() as cur:
                # Create audit log table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS audit_log (
                        id BIGSERIAL PRIMARY KEY,
                        user_id VARCHAR(100) NOT NULL,
                        operation VARCHAR(20) NOT NULL,
                        table_name VARCHAR(100) NOT NULL,
                        record_id VARCHAR(100) NOT NULL,
                        changes JSONB,
                        timestamp TIMESTAMP NOT NULL,
                        metadata JSONB
                    )
                """)
                
                # Create version history table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS version_history (
                        id BIGSERIAL PRIMARY KEY,
                        table_name VARCHAR(100) NOT NULL,
                        record_id VARCHAR(100) NOT NULL,
                        version INTEGER NOT NULL,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        created_by VARCHAR(100) NOT NULL,
                        UNIQUE (table_name, record_id, version)
                    )
                """)
    
    def track_change(
        self,
        user_id: str,
        operation: OperationType,
        table_name: str,
        record_id: Union[int, str],
        changes: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Track a change operation."""
        with self._get_db_conn() as conn:
            with conn.cursor() as cur:
                # Insert audit log
                cur.execute("""
                    INSERT INTO audit_log (
                        user_id,
                        operation,
                        table_name,
                        record_id,
                        changes,
                        timestamp,
                        metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s
                    ) RETURNING id
                """, (
                    user_id,
                    operation.value,
                    table_name,
                    str(record_id),
                    Json(changes) if changes else None,
                    datetime.now(),
                    Json(metadata) if metadata else None
                ))
                
                log_id = cur.fetchone()[0]
                
                # Store version if applicable
                if operation in (
                    OperationType.CREATE,
                    OperationType.UPDATE
                ) and changes:
                    self._store_version(
                        cur,
                        table_name,
                        record_id,
                        changes,
                        user_id
                    )
                
                return log_id
    
    def _store_version(
        self,
        cur,
        table_name: str,
        record_id: Union[int, str],
        data: Dict[str, Any],
        user_id: str
    ):
        """Store a new version."""
        # Get next version number
        cur.execute("""
            SELECT COALESCE(MAX(version), 0) + 1
            FROM version_history
            WHERE table_name = %s
            AND record_id = %s
        """, (table_name, str(record_id)))
        
        version = cur.fetchone()[0]
        
        # Store version
        cur.execute("""
            INSERT INTO version_history (
                table_name,
                record_id,
                version,
                data,
                created_at,
                created_by
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            )
        """, (
            table_name,
            str(record_id),
            version,
            Json(data),
            datetime.now(),
            user_id
        ))
    
    def get_audit_logs(
        self,
        table_name: Optional[str] = None,
        record_id: Optional[Union[int, str]] = None,
        user_id: Optional[str] = None,
        limit: int = 100
    ) -> List[AuditLog]:
        """Get audit logs with filters."""
        with self._get_db_conn() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                query = "SELECT * FROM audit_log WHERE 1=1"
                params = []
                
                if table_name:
                    query += " AND table_name = %s"
                    params.append(table_name)
                
                if record_id:
                    query += " AND record_id = %s"
                    params.append(str(record_id))
                
                if user_id:
                    query += " AND user_id = %s"
                    params.append(user_id)
                
                query += " ORDER BY timestamp DESC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                return [
                    AuditLog(
                        id=row['id'],
                        user_id=row['user_id'],
                        operation=OperationType(row['operation']),
                        table_name=row['table_name'],
                        record_id=row['record_id'],
                        changes=row['changes'],
                        timestamp=row['timestamp'],
                        metadata=row['metadata']
                    )
                    for row in rows
                ]
    
    def get_version_history(
        self,
        table_name: str,
        record_id: Union[int, str],
        version: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get version history for a record."""
        with self._get_db_conn() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                query = """
                    SELECT *
                    FROM version_history
                    WHERE table_name = %s
                    AND record_id = %s
                """
                params = [table_name, str(record_id)]
                
                if version:
                    query += " AND version = %s"
                    params.append(version)
                
                query += " ORDER BY version DESC"
                
                cur.execute(query, params)
                return cur.fetchall()
    
    def _get_db_conn(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def close(self):
        """Close connections."""
        self.redis.close()

def main():
    """Run change tracking example."""
    # Configuration
    db_config = {
        'dbname': 'testdb',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost'
    }
    
    redis_config = {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    }
    
    tracker = ChangeTracking(db_config, redis_config)
    
    try:
        # Track some changes
        tracker.track_change(
            user_id='user1',
            operation=OperationType.CREATE,
            table_name='users',
            record_id=1,
            changes={
                'name': 'John Doe',
                'email': 'john@example.com'
            }
        )
        
        tracker.track_change(
            user_id='user1',
            operation=OperationType.UPDATE,
            table_name='users',
            record_id=1,
            changes={
                'email': 'john.doe@example.com'
            }
        )
        
        # Get audit logs
        logs = tracker.get_audit_logs(
            table_name='users',
            record_id=1
        )
        
        for log in logs:
            logger.info(
                f"Audit: {log.operation.value} by {log.user_id} "
                f"at {log.timestamp}"
            )
        
        # Get version history
        versions = tracker.get_version_history(
            'users',
            1
        )
        
        for version in versions:
            logger.info(
                f"Version {version['version']}: "
                f"{version['data']}"
            )
    
    finally:
        tracker.close()

if __name__ == '__main__':
    main() 