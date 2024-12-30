"""Database operations for ETL system."""

import os
import json
from typing import Dict, List, Optional
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

class Database:
    """Database connection and operations."""
    
    def __init__(self):
        """Initialize database connection."""
        self.engine = sa.create_engine(os.getenv('DATABASE_URL'))
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def execute_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute SQL query."""
        try:
            result = self.session.execute(sa.text(query), params or {})
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            raise
    
    def insert_pipeline_run(
        self,
        pipeline_name: str,
        status: str,
        error_message: Optional[str] = None
    ) -> int:
        """Insert pipeline run record."""
        query = """
        INSERT INTO pipeline_runs (
            pipeline_name, status, start_time, error_message
        ) VALUES (
            :pipeline_name, :status, NOW(), :error_message
        ) RETURNING id;
        """
        result = self.execute_query(
            query,
            {
                'pipeline_name': pipeline_name,
                'status': status,
                'error_message': error_message
            }
        )
        return result[0]['id']
    
    def update_pipeline_run(
        self,
        run_id: int,
        status: str,
        metrics: Dict = None,
        error_message: Optional[str] = None
    ):
        """Update pipeline run record."""
        query = """
        UPDATE pipeline_runs
        SET status = :status,
            end_time = NOW(),
            metrics = :metrics,
            error_message = :error_message
        WHERE id = :run_id;
        """
        self.execute_query(
            query,
            {
                'run_id': run_id,
                'status': status,
                'metrics': json.dumps(metrics) if metrics else None,
                'error_message': error_message
            }
        ) 