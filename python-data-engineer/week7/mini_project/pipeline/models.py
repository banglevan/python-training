"""
Data Models Module
-------------

Defines data structures used in the pipeline.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from datetime import datetime

@dataclass
class SensorData:
    """Sensor data structure."""
    sensor_id: str
    metric: str
    value: float
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class ProcessedData:
    """Processed data structure."""
    sensor_id: str
    metric: str
    value: float
    timestamp: datetime
    processed_at: datetime
    aggregations: Optional[Dict[str, float]] = None
    alerts: Optional[Dict[str, Any]] = None 