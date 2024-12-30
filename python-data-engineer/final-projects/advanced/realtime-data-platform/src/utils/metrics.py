"""
Metrics utility.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import time
import logging

logger = logging.getLogger(__name__)

class MetricsTracker:
    """Metrics tracking utility."""
    
    def __init__(self):
        """Initialize tracker."""
        self.metrics = {}
        self.start_times = {}
    
    def start_operation(self, operation: str) -> None:
        """
        Start timing an operation.
        
        Args:
            operation: Operation name
        """
        self.start_times[operation] = time.time()
    
    def end_operation(self, operation: str) -> float:
        """
        End timing an operation.
        
        Args:
            operation: Operation name
        
        Returns:
            Duration in seconds
        """
        if operation not in self.start_times:
            logger.warning(f"Operation {operation} was not started")
            return 0.0
        
        duration = time.time() - self.start_times[operation]
        
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0
            }
        
        self.metrics[operation]['count'] += 1
        self.metrics[operation]['total_time'] += duration
        self.metrics[operation]['min_time'] = min(
            self.metrics[operation]['min_time'],
            duration
        )
        self.metrics[operation]['max_time'] = max(
            self.metrics[operation]['max_time'],
            duration
        )
        
        del self.start_times[operation]
        return duration
    
    def record_value(
        self,
        metric: str,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record a metric value.
        
        Args:
            metric: Metric name
            value: Metric value
            tags: Optional metric tags
        """
        if metric not in self.metrics:
            self.metrics[metric] = {
                'values': [],
                'tags': set()
            }
        
        self.metrics[metric]['values'].append({
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'tags': tags or {}
        })
        
        if tags:
            self.metrics[metric]['tags'].update(tags.keys())
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get all recorded metrics.
        
        Returns:
            Dictionary of metrics
        """
        return self.metrics
    
    def reset(self) -> None:
        """Reset all metrics."""
        self.metrics = {}
        self.start_times = {} 