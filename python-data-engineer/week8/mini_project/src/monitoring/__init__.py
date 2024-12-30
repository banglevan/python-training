"""
Monitoring package.

This package provides monitoring components:
- Metrics: System metrics collection and reporting
- Alerts: Alert management and notification
"""

from typing import Dict, Any, Optional
import logging
from enum import Enum

from .metrics import MetricsManager
from .alerts import AlertManager, AlertSeverity, AlertChannel

logger = logging.getLogger(__name__)

class MonitoringType(Enum):
    """Supported monitoring types."""
    METRICS = 'metrics'
    ALERTS = 'alerts'

class MonitoringManager:
    """Manages system monitoring components."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize monitoring manager."""
        self.config = config
        self.metrics: Optional[MetricsManager] = None
        self.alerts: Optional[AlertManager] = None
        
        # Initialize components
        self._init_components()
    
    def _init_components(self):
        """Initialize monitoring components."""
        try:
            # Initialize metrics if configured
            if 'metrics' in self.config:
                self.metrics = MetricsManager(self.config['metrics'])
                logger.info("Metrics manager initialized")
            
            # Initialize alerts if configured
            if 'alerts' in self.config:
                self.alerts = AlertManager(self.config['alerts'])
                logger.info("Alert manager initialized")
                
        except Exception as e:
            logger.error(f"Monitoring initialization failed: {e}")
            raise
    
    def record_event(
        self,
        event_type: str,
        event_data: Dict[str, Any],
        alert: bool = False,
        alert_severity: Optional[AlertSeverity] = None
    ):
        """
        Record monitoring event.

        Args:
            event_type: Type of event
            event_data: Event data
            alert: Whether to generate alert
            alert_severity: Alert severity level
        """
        try:
            # Record metrics
            if self.metrics:
                if event_type == 'sync':
                    self.metrics.record_sync_event(
                        source=event_data.get('source', 'unknown'),
                        entity_type=event_data.get('entity_type', 'unknown'),
                        status=event_data.get('status', 'unknown'),
                        duration=event_data.get('duration', 0.0)
                    )
                elif event_type == 'state':
                    self.metrics.record_state_update(
                        source=event_data.get('source', 'unknown'),
                        entity_type=event_data.get('entity_type', 'unknown'),
                        operation=event_data.get('operation', 'unknown')
                    )
                elif event_type == 'health':
                    self.metrics.update_health_status(
                        connector_name=event_data.get('name', 'unknown'),
                        connector_type=event_data.get('type', 'unknown'),
                        is_healthy=event_data.get('healthy', False)
                    )
            
            # Generate alert if requested
            if alert and self.alerts:
                self.alerts.send_alert(
                    title=f"{event_type.upper()} Event",
                    message=str(event_data),
                    severity=alert_severity,
                    metadata=event_data
                )
                
        except Exception as e:
            logger.error(f"Event recording failed: {e}")
    
    def get_component(
        self,
        component_type: MonitoringType
    ) -> Optional[Any]:
        """
        Get monitoring component.

        Args:
            component_type: Type of component to get
        """
        if component_type == MonitoringType.METRICS:
            return self.metrics
        elif component_type == MonitoringType.ALERTS:
            return self.alerts
        return None
    
    def close(self):
        """Close monitoring components."""
        try:
            if self.metrics:
                self.metrics.close()
            if self.alerts:
                # Alert manager might need cleanup in future
                pass
        except Exception as e:
            logger.error(f"Monitoring cleanup failed: {e}")

def create_monitoring(config: Dict[str, Any]) -> Optional[MonitoringManager]:
    """
    Create monitoring manager instance.

    Args:
        config: Monitoring configuration
    """
    try:
        return MonitoringManager(config)
    except Exception as e:
        logger.error(f"Monitoring creation failed: {e}")
        return None

__all__ = [
    'MetricsManager',
    'AlertManager',
    'MonitoringManager',
    'MonitoringType',
    'AlertSeverity',
    'AlertChannel',
    'create_monitoring'
] 