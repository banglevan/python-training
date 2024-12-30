"""
Custom Airflow operators exercise.
"""

from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, Optional
import requests
import json
import time

class DataValidationOperator(BaseOperator):
    """
    Custom operator to validate data quality.
    """
    
    @apply_defaults
    def __init__(
        self,
        table: str,
        validation_queries: Dict[str, str],
        postgres_conn_id: str = 'postgres_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table = table
        self.validation_queries = validation_queries
        self.postgres_conn_id = postgres_conn_id
    
    def execute(self, context: Dict[str, Any]) -> None:
        """Execute validation queries."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        failed_tests = []
        
        for test_name, query in self.validation_queries.items():
            records = hook.get_records(query.format(table=self.table))
            
            if not records or not records[0][0]:
                failed_tests.append(test_name)
        
        if failed_tests:
            raise ValueError(
                f"Data validation failed for tests: {', '.join(failed_tests)}"
            )
        
        self.log.info("All data validation tests passed!")

class APIHook(BaseHook):
    """
    Custom hook for API interactions.
    """
    
    def __init__(
        self,
        api_conn_id: str,
        retry_limit: int = 3,
        retry_delay: int = 1
    ) -> None:
        super().__init__()
        self.api_conn_id = api_conn_id
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
    
    def get_conn(self) -> requests.Session:
        """Get API connection."""
        conn = self.get_connection(self.api_conn_id)
        session = requests.Session()
        
        if conn.login:
            session.auth = (conn.login, conn.password)
        
        if conn.extra_dejson.get('headers'):
            session.headers.update(conn.extra_dejson['headers'])
        
        session.base_url = conn.host
        return session
    
    def run(
        self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute API request."""
        session = self.get_conn()
        url = f"{session.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.retry_limit):
            try:
                response = session.request(
                    method=method,
                    url=url,
                    json=data
                )
                response.raise_for_status()
                return response.json()
                
            except Exception as e:
                if attempt == self.retry_limit - 1:
                    raise
                time.sleep(self.retry_delay)

class DataAvailabilitySensor(BaseSensorOperator):
    """
    Custom sensor to check data availability.
    """
    
    @apply_defaults
    def __init__(
        self,
        table: str,
        postgres_conn_id: str = 'postgres_default',
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
    
    def poke(self, context: Dict[str, Any]) -> bool:
        """Check if data is available."""
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Check if table exists and has data
        records = hook.get_records(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = '{self.table}'
            )
            AND EXISTS (
                SELECT 1
                FROM {self.table}
                LIMIT 1
            );
        """)
        
        return records and records[0][0]

# Example usage in DAG:
"""
validation_op = DataValidationOperator(
    task_id='validate_data',
    table='users',
    validation_queries={
        'null_check': "SELECT COUNT(*) = 0 FROM {table} WHERE email IS NULL",
        'row_count': "SELECT COUNT(*) > 0 FROM {table}"
    }
)

api_hook = APIHook(api_conn_id='api_default')
data = api_hook.run('users/1')

data_sensor = DataAvailabilitySensor(
    task_id='check_data',
    table='users',
    poke_interval=60,
    timeout=3600
)
""" 