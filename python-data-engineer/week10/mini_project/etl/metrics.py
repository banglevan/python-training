"""Metrics management for ETL system."""

import os
from contextlib import contextmanager
from prometheus_client import start_http_server, Counter, Gauge, Histogram
import statsd
import time

class MetricsManager:
    """Manage metrics collection and reporting."""
    
    def __init__(self):
        """Initialize metrics manager."""
        # Start Prometheus metrics server
        self.metrics_port = int(os.getenv('METRICS_PORT', 8000))
        start_http_server(self.metrics_port)
        
        # Prometheus metrics
        self.pipeline_runs = Counter(
            'pipeline_runs_total',
            'Total number of pipeline runs',
            ['pipeline_name', 'status']
        )
        self.task_duration = Histogram(
            'task_duration_seconds',
            'Task duration in seconds',
            ['task_name']
        )
        self.error_count = Counter(
            'pipeline_errors_total',
            'Total number of pipeline errors',
            ['pipeline_name', 'error_type']
        )
        
        # StatsD client
        self.statsd = statsd.StatsClient(
            host=os.getenv('STATSD_HOST', 'localhost'),
            port=int(os.getenv('STATSD_PORT', 8125))
        )
    
    @contextmanager
    def task_duration(self, task_name: str):
        """Measure task duration."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.task_duration.labels(task_name).observe(duration)
            self.statsd.timing(f"task.{task_name}.duration", duration * 1000)
    
    def increment_task_success(self, task_name: str):
        """Increment task success counter."""
        self.statsd.incr(f"task.{task_name}.success")
    
    def increment_error(self, pipeline_name: str, error_type: str):
        """Increment error counter."""
        self.error_count.labels(
            pipeline_name=pipeline_name,
            error_type=error_type
        ).inc()
        self.statsd.incr(f"pipeline.{pipeline_name}.error")
    
    def increment_pipeline_success(self, pipeline_name: str):
        """Increment pipeline success counter."""
        self.pipeline_runs.labels(
            pipeline_name=pipeline_name,
            status='success'
        ).inc()
        self.statsd.incr(f"pipeline.{pipeline_name}.success")
    
    def increment_pipeline_failure(self, pipeline_name: str):
        """Increment pipeline failure counter."""
        self.pipeline_runs.labels(
            pipeline_name=pipeline_name,
            status='failed'
        ).inc()
        self.statsd.incr(f"pipeline.{pipeline_name}.failure") 