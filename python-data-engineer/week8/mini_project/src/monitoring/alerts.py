"""
Alert management implementation.
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import json
from enum import Enum
import smtplib
from email.mime.text import MIMEText
import requests
from prometheus_client import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'

class AlertChannel(Enum):
    """Alert notification channels."""
    EMAIL = 'email'
    SLACK = 'slack'
    WEBHOOK = 'webhook'
    PAGERDUTY = 'pagerduty'

class AlertManager:
    """Manages system alerts and notifications."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize alert manager."""
        self.config = config
        self.default_severity = AlertSeverity(
            config.get('default_severity', 'warning')
        )
        self.channels = self._setup_channels()
        
        # Alert metrics
        self.alert_counter = Counter(
            'sync_alerts_total',
            'Total alerts generated',
            ['severity', 'channel']
        )
    
    def send_alert(
        self,
        title: str,
        message: str,
        severity: Optional[AlertSeverity] = None,
        channels: Optional[List[AlertChannel]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send alert through configured channels.
        
        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity level
            channels: List of channels to use
            metadata: Additional alert metadata
        """
        try:
            severity = severity or self.default_severity
            channels = channels or self._get_channels_for_severity(severity)
            
            alert_data = {
                'id': self._generate_alert_id(),
                'title': title,
                'message': message,
                'severity': severity.value,
                'timestamp': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            
            success = True
            for channel in channels:
                try:
                    if self._send_to_channel(channel, alert_data):
                        self.alert_counter.labels(
                            severity=severity.value,
                            channel=channel.value
                        ).inc()
                    else:
                        success = False
                except Exception as e:
                    logger.error(f"Channel {channel.value} alert failed: {e}")
                    success = False
            
            return success
            
        except Exception as e:
            logger.error(f"Alert sending failed: {e}")
            return False
    
    def _setup_channels(self) -> Dict[AlertChannel, Callable]:
        """Setup alert channel handlers."""
        channels = {}
        
        # Setup email
        if 'email' in self.config:
            channels[AlertChannel.EMAIL] = self._send_email
        
        # Setup Slack
        if 'slack' in self.config:
            channels[AlertChannel.SLACK] = self._send_slack
        
        # Setup webhook
        if 'webhook' in self.config:
            channels[AlertChannel.WEBHOOK] = self._send_webhook
        
        # Setup PagerDuty
        if 'pagerduty' in self.config:
            channels[AlertChannel.PAGERDUTY] = self._send_pagerduty
        
        return channels
    
    def _get_channels_for_severity(
        self,
        severity: AlertSeverity
    ) -> List[AlertChannel]:
        """Get channels for severity level."""
        severity_channels = self.config.get('severity_channels', {})
        return [
            AlertChannel(channel)
            for channel in severity_channels.get(
                severity.value,
                ['email']  # Default to email
            )
        ]
    
    def _send_email(self, alert_data: Dict[str, Any]) -> bool:
        """Send email alert."""
        try:
            email_config = self.config['email']
            
            msg = MIMEText(
                f"{alert_data['message']}\n\n"
                f"Severity: {alert_data['severity']}\n"
                f"Time: {alert_data['timestamp']}\n"
                f"Metadata: {json.dumps(alert_data['metadata'], indent=2)}"
            )
            
            msg['Subject'] = f"[{alert_data['severity'].upper()}] {alert_data['title']}"
            msg['From'] = email_config['from']
            msg['To'] = ', '.join(email_config['to'])
            
            with smtplib.SMTP(
                email_config['host'],
                email_config.get('port', 587)
            ) as server:
                if email_config.get('tls', True):
                    server.starttls()
                
                if 'username' in email_config:
                    server.login(
                        email_config['username'],
                        email_config['password']
                    )
                
                server.send_message(msg)
            
            return True
            
        except Exception as e:
            logger.error(f"Email alert failed: {e}")
            return False
    
    def _send_slack(self, alert_data: Dict[str, Any]) -> bool:
        """Send Slack alert."""
        try:
            slack_config = self.config['slack']
            
            payload = {
                'text': f"*[{alert_data['severity'].upper()}] {alert_data['title']}*\n"
                       f"{alert_data['message']}",
                'attachments': [{
                    'fields': [
                        {
                            'title': 'Severity',
                            'value': alert_data['severity'],
                            'short': True
                        },
                        {
                            'title': 'Time',
                            'value': alert_data['timestamp'],
                            'short': True
                        }
                    ]
                }]
            }
            
            if alert_data['metadata']:
                payload['attachments'][0]['fields'].append({
                    'title': 'Metadata',
                    'value': f"```{json.dumps(alert_data['metadata'], indent=2)}```"
                })
            
            response = requests.post(
                slack_config['webhook_url'],
                json=payload
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
            return False
    
    def _send_webhook(self, alert_data: Dict[str, Any]) -> bool:
        """Send webhook alert."""
        try:
            webhook_config = self.config['webhook']
            
            response = requests.post(
                webhook_config['url'],
                json=alert_data,
                headers=webhook_config.get('headers', {})
            )
            
            return response.status_code in [200, 201, 202]
            
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")
            return False
    
    def _send_pagerduty(self, alert_data: Dict[str, Any]) -> bool:
        """Send PagerDuty alert."""
        try:
            pagerduty_config = self.config['pagerduty']
            
            payload = {
                'routing_key': pagerduty_config['routing_key'],
                'event_action': 'trigger',
                'payload': {
                    'summary': alert_data['title'],
                    'severity': alert_data['severity'],
                    'source': pagerduty_config.get('source', 'data-sync'),
                    'custom_details': {
                        'message': alert_data['message'],
                        'metadata': alert_data['metadata']
                    }
                }
            }
            
            response = requests.post(
                'https://events.pagerduty.com/v2/enqueue',
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            
            return response.status_code == 202
            
        except Exception as e:
            logger.error(f"PagerDuty alert failed: {e}")
            return False
    
    def _generate_alert_id(self) -> str:
        """Generate unique alert ID."""
        from uuid import uuid4
        return str(uuid4()) 