"""
Metrics Collection
--------------

Collects and stores:
1. Performance metrics
2. Resource usage
3. Operation statistics
4. Storage analytics
"""

from typing import Dict, Any, Optional, List, Union
import logging
from datetime import datetime, timedelta
import psutil
import os
import json
import sqlite3
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor
from prometheus_client import (
    Counter, Gauge, Histogram,
    start_http_server
)

logger = logging.getLogger(__name__)

class MetricsCollector:
    """System metrics collector."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize metrics collector."""
        self.db_path = Path('metrics.db')
        self.retention_days = config.get(
            'retention_days',
            30
        )
        self.collection_interval = config.get(
            'collection_interval',
            60
        )
        
        # Initialize Prometheus metrics
        self._init_prometheus(
            config.get('prometheus_port', 9090)
        )
        
        # Initialize database
        self._init_database()
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        logger.info("Initialized MetricsCollector")
    
    async def collect_metrics(
        self,
        storage_backends: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Collect system metrics."""
        try:
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'system': await self._get_system_metrics(),
                'storage': await self._get_storage_metrics(
                    storage_backends
                ),
                'operations': await self._get_operation_metrics()
            }
            
            # Store metrics
            await self._store_metrics(metrics)
            
            # Update Prometheus metrics
            self._update_prometheus(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            raise
    
    def get_metrics(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        metric_types: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve stored metrics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM metrics"
                params = []
                
                # Add time range filters
                if start_time or end_time:
                    conditions = []
                    if start_time:
                        conditions.append(
                            "timestamp >= ?"
                        )
                        params.append(
                            start_time.isoformat()
                        )
                    if end_time:
                        conditions.append(
                            "timestamp <= ?"
                        )
                        params.append(
                            end_time.isoformat()
                        )
                    query += " WHERE " + " AND ".join(
                        conditions
                    )
                
                # Add metric type filter
                if metric_types:
                    types_str = ",".join(
                        f"'{t}'" for t in metric_types
                    )
                    query += (
                        f" AND metric_type IN ({types_str})"
                        if "WHERE" in query
                        else f" WHERE metric_type IN ({types_str})"
                    )
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                
                return [
                    {
                        'timestamp': row[0],
                        'metric_type': row[1],
                        'metrics': json.loads(row[2])
                    }
                    for row in cursor.fetchall()
                ]
            
        except Exception as e:
            logger.error(f"Metrics retrieval failed: {e}")
            raise
    
    async def cleanup_old_metrics(self):
        """Remove old metrics data."""
        try:
            cutoff = datetime.now() - timedelta(
                days=self.retention_days
            )
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    DELETE FROM metrics
                    WHERE timestamp < ?
                    """,
                    (cutoff.isoformat(),)
                )
            
            logger.info("Old metrics cleaned up")
            
        except Exception as e:
            logger.error(f"Metrics cleanup failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown()
    
    def _init_prometheus(
        self,
        port: int
    ):
        """Initialize Prometheus metrics."""
        # Storage metrics
        self.storage_usage = Gauge(
            'storage_usage_bytes',
            'Storage space usage in bytes',
            ['backend']
        )
        self.storage_ops = Counter(
            'storage_operations_total',
            'Storage operations count',
            ['backend', 'operation']
        )
        self.operation_latency = Histogram(
            'operation_latency_seconds',
            'Operation latency in seconds',
            ['operation']
        )
        
        # System metrics
        self.cpu_usage = Gauge(
            'system_cpu_usage_percent',
            'CPU usage percentage'
        )
        self.memory_usage = Gauge(
            'system_memory_usage_bytes',
            'Memory usage in bytes'
        )
        self.io_usage = Gauge(
            'system_io_usage_bytes',
            'IO usage in bytes',
            ['type']
        )
        
        # Start Prometheus HTTP server
        start_http_server(port)
    
    def _init_database(self):
        """Initialize SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS metrics (
                        timestamp TEXT NOT NULL,
                        metric_type TEXT NOT NULL,
                        metrics TEXT NOT NULL
                    )
                    """
                )
                
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_metrics_timestamp
                    ON metrics(timestamp)
                    """
                )
            
        except Exception as e:
            logger.error(
                f"Database initialization failed: {e}"
            )
            raise
    
    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Collect system metrics."""
        return {
            'cpu': {
                'percent': psutil.cpu_percent(
                    interval=1
                ),
                'count': psutil.cpu_count(),
                'freq': psutil.cpu_freq()._asdict()
            },
            'memory': psutil.virtual_memory()._asdict(),
            'disk': {
                path: psutil.disk_usage(path)._asdict()
                for path in psutil.disk_partitions()
            },
            'network': psutil.net_io_counters()._asdict(),
            'io': psutil.disk_io_counters()._asdict()
        }
    
    async def _get_storage_metrics(
        self,
        storage_backends: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Collect storage backend metrics."""
        metrics = {}
        
        for name, storage in storage_backends.items():
            try:
                stats = await asyncio.to_thread(
                    storage.get_stats
                )
                metrics[name] = stats
            except Exception as e:
                logger.error(
                    f"Failed to get metrics for "
                    f"{name}: {e}"
                )
        
        return metrics
    
    async def _get_operation_metrics(self) -> Dict[str, Any]:
        """Collect operation metrics."""
        # This would track operation counts, latencies, etc.
        # Implementation depends on how operations are tracked
        return {}
    
    async def _store_metrics(
        self,
        metrics: Dict[str, Any]
    ):
        """Store collected metrics."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for metric_type, data in metrics.items():
                    if metric_type != 'timestamp':
                        cursor.execute(
                            """
                            INSERT INTO metrics
                            (timestamp, metric_type, metrics)
                            VALUES (?, ?, ?)
                            """,
                            (
                                metrics['timestamp'],
                                metric_type,
                                json.dumps(data)
                            )
                        )
            
        except Exception as e:
            logger.error(f"Metrics storage failed: {e}")
            raise
    
    def _update_prometheus(
        self,
        metrics: Dict[str, Any]
    ):
        """Update Prometheus metrics."""
        try:
            # Update system metrics
            system = metrics['system']
            self.cpu_usage.set(
                system['cpu']['percent']
            )
            self.memory_usage.set(
                system['memory']['used']
            )
            self.io_usage.labels(
                type='read'
            ).set(system['io']['read_bytes'])
            self.io_usage.labels(
                type='write'
            ).set(system['io']['write_bytes'])
            
            # Update storage metrics
            for backend, stats in metrics['storage'].items():
                self.storage_usage.labels(
                    backend=backend
                ).set(stats.get('used_space', 0))
            
        except Exception as e:
            logger.error(
                f"Prometheus update failed: {e}"
            )
            raise 