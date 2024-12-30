"""
CDC (Change Data Capture) Patterns
-----------------------------

Practice with:
1. Implementation strategies
2. Change detection
3. Data versioning
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
import hashlib
from dataclasses import dataclass
from enum import Enum
import psycopg2
from psycopg2.extras import DictCursor
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChangeType(Enum):
    """Types of changes."""
    INSERT = 'INSERT'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'

@dataclass
class Change:
    """Change record structure."""
    table: str
    primary_key: Dict[str, Any]
    change_type: ChangeType
    old_data: Optional[Dict[str, Any]]
    new_data: Optional[Dict[str, Any]]
    timestamp: datetime
    version: int
    checksum: str

class CDCPatterns:
    """CDC patterns implementation."""
    
    def __init__(
        self,
        db_config: Dict[str, Any],
        redis_config: Dict[str, Any]
    ):
        """Initialize CDC handler."""
        self.db_config = db_config
        self.redis = redis.Redis(**redis_config)
        
        # Create tables if needed
        self._init_tables()
    
    def _init_tables(self):
        """Initialize required tables."""
        with self._get_db_conn() as conn:
            with conn.cursor() as cur:
                # Create change log table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS change_log (
                        id BIGSERIAL PRIMARY KEY,
                        table_name VARCHAR(100) NOT NULL,
                        primary_key JSONB NOT NULL,
                        change_type VARCHAR(20) NOT NULL,
                        old_data JSONB,
                        new_data JSONB,
                        timestamp TIMESTAMP NOT NULL,
                        version INTEGER NOT NULL,
                        checksum VARCHAR(64) NOT NULL
                    )
                """)
                
                # Create trigger function
                cur.execute("""
                    CREATE OR REPLACE FUNCTION log_changes()
                    RETURNS TRIGGER AS $$
                    DECLARE
                        old_data JSONB;
                        new_data JSONB;
                        pk_data JSONB;
                    BEGIN
                        -- Get primary key data
                        IF TG_OP = 'DELETE' THEN
                            pk_data = row_to_json(OLD)::JSONB;
                        ELSE
                            pk_data = row_to_json(NEW)::JSONB;
                        END IF;
                        
                        -- Get old and new data
                        IF TG_OP = 'UPDATE' THEN
                            old_data = row_to_json(OLD)::JSONB;
                            new_data = row_to_json(NEW)::JSONB;
                        ELSIF TG_OP = 'INSERT' THEN
                            new_data = row_to_json(NEW)::JSONB;
                        ELSIF TG_OP = 'DELETE' THEN
                            old_data = row_to_json(OLD)::JSONB;
                        END IF;
                        
                        -- Insert change log
                        INSERT INTO change_log (
                            table_name,
                            primary_key,
                            change_type,
                            old_data,
                            new_data,
                            timestamp,
                            version,
                            checksum
                        ) VALUES (
                            TG_TABLE_NAME,
                            pk_data,
                            TG_OP,
                            old_data,
                            new_data,
                            CURRENT_TIMESTAMP,
                            COALESCE(
                                (SELECT MAX(version) + 1
                                FROM change_log
                                WHERE table_name = TG_TABLE_NAME
                                AND primary_key = pk_data),
                                1
                            ),
                            encode(
                                digest(
                                    COALESCE(new_data::text, old_data::text),
                                    'sha256'
                                ),
                                'hex'
                            )
                        );
                        
                        RETURN NULL;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
    
    def enable_cdc(
        self,
        table_name: str,
        pk_columns: List[str]
    ):
        """Enable CDC for a table."""
        with self._get_db_conn() as conn:
            with conn.cursor() as cur:
                # Create trigger
                cur.execute(f"""
                    DROP TRIGGER IF EXISTS {table_name}_changes 
                    ON {table_name}
                """)
                
                cur.execute(f"""
                    CREATE TRIGGER {table_name}_changes
                    AFTER INSERT OR UPDATE OR DELETE ON {table_name}
                    FOR EACH ROW EXECUTE FUNCTION log_changes()
                """)
                
                logger.info(f"CDC enabled for table: {table_name}")
    
    def get_changes(
        self,
        table_name: str,
        since_version: Optional[int] = None,
        limit: int = 100
    ) -> List[Change]:
        """Get changes for a table."""
        with self._get_db_conn() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                query = """
                    SELECT *
                    FROM change_log
                    WHERE table_name = %s
                """
                params = [table_name]
                
                if since_version:
                    query += " AND version > %s"
                    params.append(since_version)
                
                query += " ORDER BY version ASC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                return [
                    Change(
                        table=row['table_name'],
                        primary_key=row['primary_key'],
                        change_type=ChangeType(row['change_type']),
                        old_data=row['old_data'],
                        new_data=row['new_data'],
                        timestamp=row['timestamp'],
                        version=row['version'],
                        checksum=row['checksum']
                    )
                    for row in rows
                ]
    
    def verify_changes(
        self,
        changes: List[Change]
    ) -> List[Tuple[Change, bool]]:
        """Verify change integrity."""
        results = []
        
        for change in changes:
            # Calculate checksum
            data = (
                change.new_data if change.new_data
                else change.old_data
            )
            calculated = hashlib.sha256(
                json.dumps(data, sort_keys=True).encode()
            ).hexdigest()
            
            # Compare checksums
            valid = calculated == change.checksum
            results.append((change, valid))
            
            if not valid:
                logger.warning(
                    f"Invalid checksum for change: "
                    f"table={change.table}, "
                    f"version={change.version}"
                )
        
        return results
    
    def _get_db_conn(self):
        """Get database connection."""
        return psycopg2.connect(**self.db_config)
    
    def close(self):
        """Close connections."""
        self.redis.close()

def main():
    """Run CDC patterns example."""
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
    
    cdc = CDCPatterns(db_config, redis_config)
    
    try:
        # Enable CDC for table
        cdc.enable_cdc(
            'users',
            ['id']
        )
        
        # Get and verify changes
        changes = cdc.get_changes(
            'users',
            since_version=0,
            limit=10
        )
        
        results = cdc.verify_changes(changes)
        
        for change, is_valid in results:
            logger.info(
                f"Change: table={change.table}, "
                f"type={change.change_type.value}, "
                f"version={change.version}, "
                f"valid={is_valid}"
            )
    
    finally:
        cdc.close()

if __name__ == '__main__':
    main() 