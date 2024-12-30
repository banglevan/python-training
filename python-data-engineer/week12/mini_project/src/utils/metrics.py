"""
Metrics collection utilities.
"""

from typing import Dict, Any, Optional
import logging
from datetime import datetime
from prometheus_client import Counter, Gauge, Histogram
from src.init.platform import platform

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define metrics
QUERY_COUNTER = Counter(
    'query_total',
    'Total number of queries executed',
    ['status']
)

QUERY_LATENCY = Histogram(
    'query_latency_seconds',
    'Query execution latency',
    ['query_type']
)

STORAGE_SIZE = Gauge(
    'storage_size_bytes',
    'Storage size in bytes',
    ['storage_type']
)

class MetricsCollector:
    """Metrics collection management."""
    
    @staticmethod
    def record_query(
        query_type: str,
        status: str,
        duration: float
    ) -> None:
        """Record query metrics."""
        try:
            QUERY_COUNTER.labels(status=status).inc()
            QUERY_LATENCY.labels(query_type=query_type).observe(duration)
            
            # Store in Delta table
            spark = platform.get_component('spark')
            spark.sql(f"""
                INSERT INTO metrics VALUES (
                    '{datetime.now().isoformat()}',
                    'query_execution',
                    {duration},
                    map('type', '{query_type}', 'status', '{status}')
                )
            """)
            
        except Exception as e:
            logger.error(f"Failed to record query metrics: {e}")
    
    @staticmethod
    def update_storage_metrics() -> None:
        """Update storage metrics."""
        try:
            spark = platform.get_component('spark')
            delta_manager = platform.get_component('delta_manager')
            
            # Get Delta storage size
            delta_size = 0
            for table in delta_manager.tables:
                stats = delta_manager.get_table_stats(table)
                delta_size += stats['size_bytes']
            
            STORAGE_SIZE.labels(storage_type='delta').set(delta_size)
            
            # Store in metrics table
            spark.sql(f"""
                INSERT INTO metrics VALUES (
                    '{datetime.now().isoformat()}',
                    'storage_size',
                    {delta_size},
                    map('type', 'delta')
                )
            """)
            
        except Exception as e:
            logger.error(f"Failed to update storage metrics: {e}") 