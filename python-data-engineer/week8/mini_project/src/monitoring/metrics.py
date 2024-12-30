"""
Metrics collection and reporting implementation.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import time
from dataclasses import dataclass
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    CollectorRegistry, push_to_gateway,
    start_http_server
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MetricPoint:
    """Metric data point structure."""
    metric_id: str
    name: str
    value: float
    labels: Dict[str, str]
    timestamp: datetime

class MetricsManager:
    """Manages system metrics collection and reporting."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize metrics manager."""
        self.config = config
        self.db_config = config['database']
        self.registry = CollectorRegistry()
        
        # Initialize Prometheus metrics
        self._setup_metrics()
        
        # Start metrics server if configured
        if config.get('server', {}).get('enabled', False):
            server_port = config['server'].get('port', 8000)
            start_http_server(server_port, registry=self.registry)
            logger.info(f"Metrics server started on port {server_port}")
    
    def _setup_metrics(self):
        """Setup Prometheus metrics."""
        # Sync metrics
        self.sync_events = Counter(
            'sync_events_total',
            'Total sync events processed',
            ['source', 'entity_type', 'status'],
            registry=self.registry
        )
        
        self.sync_latency = Histogram(
            'sync_latency_seconds',
            'Sync processing latency',
            ['source', 'entity_type'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
            registry=self.registry
        )
        
        self.sync_batch_size = Histogram(
            'sync_batch_size',
            'Sync batch size distribution',
            ['source'],
            buckets=[10, 50, 100, 500, 1000],
            registry=self.registry
        )
        
        # State metrics
        self.state_updates = Counter(
            'state_updates_total',
            'Total state updates',
            ['source', 'entity_type', 'operation'],
            registry=self.registry
        )
        
        self.state_conflicts = Counter(
            'state_conflicts_total',
            'Total state conflicts',
            ['source', 'entity_type', 'conflict_type'],
            registry=self.registry
        )
        
        # Cache metrics
        self.cache_operations = Counter(
            'cache_operations_total',
            'Total cache operations',
            ['operation', 'status'],
            registry=self.registry
        )
        
        self.cache_hit_ratio = Gauge(
            'cache_hit_ratio',
            'Cache hit ratio',
            ['cache_type'],
            registry=self.registry
        )
        
        # System metrics
        self.connector_health = Gauge(
            'connector_health',
            'Connector health status',
            ['connector_name', 'type'],
            registry=self.registry
        )
        
        self.queue_size = Gauge(
            'queue_size',
            'Queue size by type',
            ['queue_type'],
            registry=self.registry
        )
        
        # API metrics
        self.api_requests = Counter(
            'api_requests_total',
            'Total API requests',
            ['endpoint', 'method', 'status'],
            registry=self.registry
        )
        
        self.api_latency = Summary(
            'api_latency_seconds',
            'API request latency',
            ['endpoint', 'method'],
            registry=self.registry
        )
    
    def record_sync_event(
        self,
        source: str,
        entity_type: str,
        status: str,
        duration: float
    ):
        """Record sync event metrics."""
        try:
            self.sync_events.labels(
                source=source,
                entity_type=entity_type,
                status=status
            ).inc()
            
            self.sync_latency.labels(
                source=source,
                entity_type=entity_type
            ).observe(duration)
            
            self._store_metric(
                name='sync_event',
                value=duration,
                labels={
                    'source': source,
                    'entity_type': entity_type,
                    'status': status
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to record sync event: {e}")
    
    def record_state_update(
        self,
        source: str,
        entity_type: str,
        operation: str
    ):
        """Record state update metrics."""
        try:
            self.state_updates.labels(
                source=source,
                entity_type=entity_type,
                operation=operation
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record state update: {e}")
    
    def record_conflict(
        self,
        source: str,
        entity_type: str,
        conflict_type: str
    ):
        """Record conflict metrics."""
        try:
            self.state_conflicts.labels(
                source=source,
                entity_type=entity_type,
                conflict_type=conflict_type
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record conflict: {e}")
    
    def update_health_status(
        self,
        connector_name: str,
        connector_type: str,
        is_healthy: bool
    ):
        """Update connector health status."""
        try:
            self.connector_health.labels(
                connector_name=connector_name,
                type=connector_type
            ).set(1 if is_healthy else 0)
            
            self._store_metric(
                name='connector_health',
                value=1 if is_healthy else 0,
                labels={
                    'connector_name': connector_name,
                    'type': connector_type
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to update health status: {e}")
    
    def update_queue_size(self, queue_type: str, size: int):
        """Update queue size metric."""
        try:
            self.queue_size.labels(queue_type=queue_type).set(size)
            
        except Exception as e:
            logger.error(f"Failed to update queue size: {e}")
    
    def record_api_request(
        self,
        endpoint: str,
        method: str,
        status: str,
        duration: float
    ):
        """Record API request metrics."""
        try:
            self.api_requests.labels(
                endpoint=endpoint,
                method=method,
                status=status
            ).inc()
            
            self.api_latency.labels(
                endpoint=endpoint,
                method=method
            ).observe(duration)
            
        except Exception as e:
            logger.error(f"Failed to record API request: {e}")
    
    def update_cache_metrics(
        self,
        operation: str,
        status: str,
        hit_ratio: Optional[float] = None
    ):
        """Update cache metrics."""
        try:
            self.cache_operations.labels(
                operation=operation,
                status=status
            ).inc()
            
            if hit_ratio is not None:
                self.cache_hit_ratio.labels(
                    cache_type='redis'
                ).set(hit_ratio)
            
        except Exception as e:
            logger.error(f"Failed to update cache metrics: {e}")
    
    def _store_metric(
        self,
        name: str,
        value: float,
        labels: Dict[str, Any]
    ):
        """Store metric in database."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO sync_metrics
                        (metric_id, metric_name, metric_value,
                         labels, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        str(uuid.uuid4()),
                        name,
                        value,
                        psycopg2.extras.Json(labels),
                        datetime.now()
                    ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to store metric: {e}")
    
    def get_metrics_summary(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get metrics summary for time range."""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT metric_name,
                               COUNT(*) as count,
                               AVG(metric_value) as avg_value,
                               MIN(metric_value) as min_value,
                               MAX(metric_value) as max_value
                        FROM sync_metrics
                        WHERE timestamp BETWEEN %s AND %s
                        GROUP BY metric_name
                    """, (start_time, end_time))
                    
                    return {
                        row['metric_name']: {
                            'count': row['count'],
                            'avg': float(row['avg_value']),
                            'min': float(row['min_value']),
                            'max': float(row['max_value'])
                        }
                        for row in cur.fetchall()
                    }
            
        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            return {}
    
    def push_to_gateway(self):
        """Push metrics to Prometheus Pushgateway."""
        try:
            if 'pushgateway' in self.config:
                push_to_gateway(
                    self.config['pushgateway']['url'],
                    job='data_sync',
                    registry=self.registry
                )
        except Exception as e:
            logger.error(f"Failed to push to gateway: {e}")
    
    def close(self):
        """Cleanup metrics manager."""
        try:
            # Perform any necessary cleanup
            pass
        except Exception as e:
            logger.error(f"Metrics cleanup failed: {e}") 