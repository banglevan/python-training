"""
Metrics collector for feature platform monitoring.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import time
import threading
from prometheus_client import Counter, Histogram, Gauge
import logging

logger = logging.getLogger(__name__)

class MetricsCollector:
    """Collect and expose platform metrics."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single metrics collector."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize metrics."""
        if not hasattr(self, 'initialized'):
            # Feature computation metrics
            self.feature_computation_duration = Histogram(
                'feature_computation_duration_seconds',
                'Time spent computing features',
                ['feature_view']
            )
            self.features_computed = Counter(
                'features_computed_total',
                'Total number of features computed',
                ['feature_view']
            )
            self.computation_errors = Counter(
                'feature_computation_errors_total',
                'Total number of feature computation errors',
                ['feature_view']
            )
            
            # Feature validation metrics
            self.validation_failures = Counter(
                'feature_validation_failures_total',
                'Total number of feature validation failures',
                ['feature_view', 'check_type']
            )
            self.feature_freshness = Gauge(
                'feature_freshness_seconds',
                'Time since last feature update',
                ['feature_view']
            )
            
            # Serving metrics
            self.serving_latency = Histogram(
                'feature_serving_latency_seconds',
                'Feature serving latency',
                ['endpoint']
            )
            self.requests_total = Counter(
                'feature_requests_total',
                'Total number of feature requests',
                ['endpoint']
            )
            self.cache_hits = Counter(
                'feature_cache_hits_total',
                'Total number of cache hits',
                ['cache_type']
            )
            
            # Storage metrics
            self.storage_operations = Counter(
                'feature_storage_operations_total',
                'Total number of storage operations',
                ['operation', 'store']
            )
            self.storage_errors = Counter(
                'feature_storage_errors_total',
                'Total number of storage errors',
                ['store']
            )
            
            self.operation_timers = {}
            self.initialized = True
    
    def start_operation(self, operation_name: str) -> None:
        """Start timing an operation."""
        self.operation_timers[operation_name] = time.time()
    
    def end_operation(self, operation_name: str) -> float:
        """End timing an operation and return duration."""
        start_time = self.operation_timers.pop(operation_name, None)
        if start_time is None:
            return 0
        
        duration = time.time() - start_time
        return duration
    
    def record_computation(
        self,
        feature_view: str,
        duration: float
    ) -> None:
        """Record feature computation metrics."""
        try:
            self.feature_computation_duration.labels(
                feature_view=feature_view
            ).observe(duration)
            
            self.features_computed.labels(
                feature_view=feature_view
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record computation metrics: {e}")
    
    def record_validation_failure(
        self,
        feature_view: str,
        check_type: str
    ) -> None:
        """Record feature validation failure."""
        try:
            self.validation_failures.labels(
                feature_view=feature_view,
                check_type=check_type
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record validation metrics: {e}")
    
    def update_feature_freshness(
        self,
        feature_view: str,
        timestamp: datetime
    ) -> None:
        """Update feature freshness gauge."""
        try:
            freshness = (datetime.now() - timestamp).total_seconds()
            self.feature_freshness.labels(
                feature_view=feature_view
            ).set(freshness)
            
        except Exception as e:
            logger.error(f"Failed to update feature freshness: {e}")
    
    def record_serving_latency(
        self,
        endpoint: str,
        duration: float
    ) -> None:
        """Record feature serving latency."""
        try:
            self.serving_latency.labels(
                endpoint=endpoint
            ).observe(duration)
            
            self.requests_total.labels(
                endpoint=endpoint
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record serving metrics: {e}")
    
    def record_cache_hit(self, cache_type: str) -> None:
        """Record cache hit."""
        try:
            self.cache_hits.labels(
                cache_type=cache_type
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record cache metrics: {e}")
    
    def record_storage_operation(
        self,
        operation: str,
        store: str
    ) -> None:
        """Record storage operation."""
        try:
            self.storage_operations.labels(
                operation=operation,
                store=store
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record storage metrics: {e}")
    
    def record_storage_error(self, store: str) -> None:
        """Record storage error."""
        try:
            self.storage_errors.labels(
                store=store
            ).inc()
            
        except Exception as e:
            logger.error(f"Failed to record storage error: {e}") 