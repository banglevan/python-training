"""
Alert manager for monitoring system health.
"""

from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import requests
from dataclasses import dataclass
from src.utils.config import Config
from src.utils.logging import get_logger
from src.utils.metrics import MetricsTracker

logger = get_logger(__name__)

@dataclass
class Alert:
    """Alert definition."""
    name: str
    severity: str
    message: str
    timestamp: str
    tags: Dict[str, str]
    value: float
    threshold: float

class AlertManager:
    """Manager for system alerts."""
    
    def __init__(self, config: Config):
        """Initialize manager."""
        self.config = config
        self.metrics = MetricsTracker()
        
        # Alert thresholds
        self.thresholds = {
            'processing_lag': 60.0,  # seconds
            'error_rate': 0.01,      # 1%
            'cpu_usage': 80.0,       # 80%
            'memory_usage': 80.0,    # 80%
            'disk_usage': 80.0       # 80%
        }
        
        # Alert channels
        self.slack_webhook = config.monitoring.slack_webhook
        self.email_config = config.monitoring.email
    
    def check_processing_lag(self, lag_seconds: float) -> Optional[Alert]:
        """Check processing lag."""
        if lag_seconds > self.thresholds['processing_lag']:
            return Alert(
                name='high_processing_lag',
                severity='warning',
                message=f'Processing lag of {lag_seconds:.1f}s exceeds threshold of {self.thresholds["processing_lag"]}s',
                timestamp=datetime.now().isoformat(),
                tags={'metric': 'processing_lag'},
                value=lag_seconds,
                threshold=self.thresholds['processing_lag']
            )
        return None
    
    def check_error_rate(
        self,
        errors: int,
        total: int
    ) -> Optional[Alert]:
        """Check error rate."""
        if total == 0:
            return None
            
        error_rate = errors / total
        if error_rate > self.thresholds['error_rate']:
            return Alert(
                name='high_error_rate',
                severity='critical',
                message=f'Error rate of {error_rate:.2%} exceeds threshold of {self.thresholds["error_rate"]:.2%}',
                timestamp=datetime.now().isoformat(),
                tags={'metric': 'error_rate'},
                value=error_rate,
                threshold=self.thresholds['error_rate']
            )
        return None
    
    def check_resource_usage(
        self,
        metrics: Dict[str, float]
    ) -> List[Alert]:
        """Check resource usage."""
        alerts = []
        
        # Check CPU usage
        if metrics['cpu_percent'] > self.thresholds['cpu_usage']:
            alerts.append(Alert(
                name='high_cpu_usage',
                severity='warning',
                message=f'CPU usage of {metrics["cpu_percent"]}% exceeds threshold of {self.thresholds["cpu_usage"]}%',
                timestamp=datetime.now().isoformat(),
                tags={'metric': 'cpu_usage'},
                value=metrics['cpu_percent'],
                threshold=self.thresholds['cpu_usage']
            ))
        
        # Check memory usage
        if metrics['memory_percent'] > self.thresholds['memory_usage']:
            alerts.append(Alert(
                name='high_memory_usage',
                severity='warning',
                message=f'Memory usage of {metrics["memory_percent"]}% exceeds threshold of {self.thresholds["memory_usage"]}%',
                timestamp=datetime.now().isoformat(),
                tags={'metric': 'memory_usage'},
                value=metrics['memory_percent'],
                threshold=self.thresholds['memory_usage']
            ))
        
        # Check disk usage
        if metrics['disk_usage_percent'] > self.thresholds['disk_usage']:
            alerts.append(Alert(
                name='high_disk_usage',
                severity='warning',
                message=f'Disk usage of {metrics["disk_usage_percent"]}% exceeds threshold of {self.thresholds["disk_usage"]}%',
                timestamp=datetime.now().isoformat(),
                tags={'metric': 'disk_usage'},
                value=metrics['disk_usage_percent'],
                threshold=self.thresholds['disk_usage']
            ))
        
        return alerts
    
    def send_alert(self, alert: Alert) -> None:
        """Send alert to configured channels."""
        try:
            self.metrics.start_operation('send_alert')
            
            # Send to Slack
            if self.slack_webhook:
                self._send_slack_alert(alert)
            
            # Send email
            if self.email_config:
                self._send_email_alert(alert)
            
            # Store in Elasticsearch
            self._store_alert(alert)
            
            duration = self.metrics.end_operation('send_alert')
            logger.info(
                f"Sent alert {alert.name} in {duration:.3f} seconds"
            )
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            raise
    
    def _send_slack_alert(self, alert: Alert) -> None:
        """Send alert to Slack."""
        try:
            payload = {
                "text": f"*{alert.severity.upper()}: {alert.name}*\n{alert.message}",
                "attachments": [{
                    "fields": [
                        {"title": "Value", "value": str(alert.value), "short": True},
                        {"title": "Threshold", "value": str(alert.threshold), "short": True}
                    ]
                }]
            }
            
            response = requests.post(
                self.slack_webhook,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            raise
    
    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert email."""
        # Implement email sending logic here
        pass
    
    def _store_alert(self, alert: Alert) -> None:
        """Store alert in Elasticsearch."""
        try:
            es_client = self.config.get_elasticsearch_client()
            
            document = {
                'name': alert.name,
                'severity': alert.severity,
                'message': alert.message,
                'timestamp': alert.timestamp,
                'tags': alert.tags,
                'value': alert.value,
                'threshold': alert.threshold
            }
            
            es_client.index(
                index='alerts',
                document=document
            )
            
        except Exception as e:
            logger.error(f"Failed to store alert: {e}")
            raise 