"""
Alert manager for monitoring system.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import requests
from dataclasses import dataclass
import logging
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

@dataclass
class Alert:
    """Alert definition."""
    name: str
    severity: str
    message: str
    timestamp: datetime
    tags: Dict[str, str]
    value: float
    threshold: float

class AlertManager:
    """Manage monitoring alerts."""
    
    def __init__(
        self,
        slack_webhook: Optional[str] = None,
        email_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize alert manager.
        
        Args:
            slack_webhook: Slack webhook URL
            email_config: Email configuration
        """
        self.slack_webhook = slack_webhook
        self.email_config = email_config
        self.metrics = MetricsCollector()
        
        # Define alert rules
        self.rules = {
            'feature_freshness': {
                'threshold': 3600,  # 1 hour
                'severity': 'high'
            },
            'validation_failures': {
                'threshold': 10,
                'severity': 'high'
            },
            'serving_latency': {
                'threshold': 1.0,  # 1 second
                'severity': 'medium'
            },
            'error_rate': {
                'threshold': 0.01,  # 1%
                'severity': 'high'
            }
        }
    
    def check_alerts(self) -> List[Alert]:
        """
        Check for alert conditions.
        
        Returns:
            List of triggered alerts
        """
        try:
            alerts = []
            
            # Check feature freshness
            for feature_view in ['customer_features', 'product_features']:
                freshness = self.metrics.feature_freshness.labels(
                    feature_view=feature_view
                )._value.get()
                
                if freshness > self.rules['feature_freshness']['threshold']:
                    alerts.append(Alert(
                        name='feature_freshness',
                        severity=self.rules['feature_freshness']['severity'],
                        message=f'Features are stale for {feature_view}',
                        timestamp=datetime.now(),
                        tags={'feature_view': feature_view},
                        value=freshness,
                        threshold=self.rules['feature_freshness']['threshold']
                    ))
            
            # Check validation failures
            for feature_view in ['customer_features', 'product_features']:
                failures = sum([
                    self.metrics.validation_failures.labels(
                        feature_view=feature_view,
                        check_type=check_type
                    )._value.get()
                    for check_type in ['missing', 'bounds', 'correlation']
                ])
                
                if failures > self.rules['validation_failures']['threshold']:
                    alerts.append(Alert(
                        name='validation_failures',
                        severity=self.rules['validation_failures']['severity'],
                        message=f'High validation failures for {feature_view}',
                        timestamp=datetime.now(),
                        tags={'feature_view': feature_view},
                        value=failures,
                        threshold=self.rules['validation_failures']['threshold']
                    ))
            
            # Check serving latency
            for endpoint in ['/features/online', '/features/historical']:
                latency = self.metrics.serving_latency.labels(
                    endpoint=endpoint
                )._value.get()
                
                if latency > self.rules['serving_latency']['threshold']:
                    alerts.append(Alert(
                        name='serving_latency',
                        severity=self.rules['serving_latency']['severity'],
                        message=f'High serving latency for {endpoint}',
                        timestamp=datetime.now(),
                        tags={'endpoint': endpoint},
                        value=latency,
                        threshold=self.rules['serving_latency']['threshold']
                    ))
            
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to check alerts: {e}")
            raise
    
    def send_alert(self, alert: Alert) -> None:
        """
        Send alert notification.
        
        Args:
            alert: Alert to send
        """
        try:
            # Send to Slack
            if self.slack_webhook:
                self._send_slack_alert(alert)
            
            # Send email
            if self.email_config:
                self._send_email_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            raise
    
    def _send_slack_alert(self, alert: Alert) -> None:
        """Send alert to Slack."""
        try:
            message = {
                "text": f"*{alert.severity.upper()} Alert: {alert.name}*\n"
                       f"Message: {alert.message}\n"
                       f"Value: {alert.value:.2f} (threshold: {alert.threshold})\n"
                       f"Tags: {json.dumps(alert.tags)}\n"
                       f"Time: {alert.timestamp.isoformat()}"
            }
            
            response = requests.post(
                self.slack_webhook,
                json=message
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            raise
    
    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert via email."""
        try:
            # Implement email sending logic here
            pass
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            raise 