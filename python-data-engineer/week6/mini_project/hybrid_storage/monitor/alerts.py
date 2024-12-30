"""
Alert Manager
-----------

Manages system alerts with:
1. Alert rules
2. Notification channels
3. Alert history
4. Alert status tracking
"""

from typing import Dict, Any, Optional, List, Union, Callable
import logging
from datetime import datetime, timedelta
import json
import sqlite3
from pathlib import Path
import asyncio
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email.message import EmailMessage
import requests

logger = logging.getLogger(__name__)

class AlertManager:
    """System alert manager."""
    
    def __init__(
        self,
        config: Dict[str, Any]
    ):
        """Initialize alert manager."""
        self.db_path = Path('alerts.db')
        self.config = config
        self.thresholds = config.get('thresholds', {})
        self.channels = self._init_channels(
            config.get('channels', {})
        )
        
        # Initialize database
        self._init_database()
        
        # Initialize thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=config.get('threads', 4)
        )
        
        logger.info("Initialized AlertManager")
    
    async def check_alerts(
        self,
        metrics: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Check metrics against alert rules."""
        try:
            alerts = []
            
            # Check storage usage
            if 'storage' in metrics:
                for backend, stats in metrics['storage'].items():
                    usage_pct = stats.get('used_percent', 0)
                    threshold = self.thresholds.get(
                        'storage_usage',
                        85
                    )
                    
                    if usage_pct > threshold:
                        alerts.append({
                            'type': 'storage_usage',
                            'severity': 'warning',
                            'backend': backend,
                            'message': (
                                f"Storage usage at {usage_pct:.1f}% "
                                f"(threshold: {threshold}%)"
                            ),
                            'timestamp': datetime.now().isoformat()
                        })
            
            # Check system metrics
            if 'system' in metrics:
                system = metrics['system']
                
                # CPU usage
                cpu_pct = system['cpu']['percent']
                cpu_threshold = self.thresholds.get(
                    'cpu_usage',
                    90
                )
                
                if cpu_pct > cpu_threshold:
                    alerts.append({
                        'type': 'cpu_usage',
                        'severity': 'warning',
                        'message': (
                            f"CPU usage at {cpu_pct}% "
                            f"(threshold: {cpu_threshold}%)"
                        ),
                        'timestamp': datetime.now().isoformat()
                    })
                
                # Memory usage
                mem = system['memory']
                mem_pct = mem['percent']
                mem_threshold = self.thresholds.get(
                    'memory_usage',
                    90
                )
                
                if mem_pct > mem_threshold:
                    alerts.append({
                        'type': 'memory_usage',
                        'severity': 'warning',
                        'message': (
                            f"Memory usage at {mem_pct}% "
                            f"(threshold: {mem_threshold}%)"
                        ),
                        'timestamp': datetime.now().isoformat()
                    })
            
            # Store and notify if there are alerts
            if alerts:
                await self._store_alerts(alerts)
                await self._send_notifications(alerts)
            
            return alerts
            
        except Exception as e:
            logger.error(f"Alert check failed: {e}")
            raise
    
    def get_alerts(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        alert_types: Optional[List[str]] = None,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Retrieve stored alerts."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                query = "SELECT * FROM alerts"
                params = []
                conditions = []
                
                if start_time:
                    conditions.append("timestamp >= ?")
                    params.append(start_time.isoformat())
                if end_time:
                    conditions.append("timestamp <= ?")
                    params.append(end_time.isoformat())
                if alert_types:
                    types_str = ",".join(
                        f"'{t}'" for t in alert_types
                    )
                    conditions.append(
                        f"type IN ({types_str})"
                    )
                if severity:
                    conditions.append("severity = ?")
                    params.append(severity)
                
                if conditions:
                    query += " WHERE " + " AND ".join(
                        conditions
                    )
                
                query += " ORDER BY timestamp DESC"
                
                cursor.execute(query, params)
                
                return [
                    {
                        'id': row[0],
                        'type': row[1],
                        'severity': row[2],
                        'message': row[3],
                        'timestamp': row[4],
                        'metadata': json.loads(row[5])
                    }
                    for row in cursor.fetchall()
                ]
            
        except Exception as e:
            logger.error(f"Alert retrieval failed: {e}")
            raise
    
    async def resolve_alert(
        self,
        alert_id: int,
        resolution: str
    ):
        """Mark alert as resolved."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    UPDATE alerts
                    SET resolved = ?,
                        resolution = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (
                        True,
                        resolution,
                        datetime.now().isoformat(),
                        alert_id
                    )
                )
            
            logger.info(f"Alert {alert_id} resolved")
            
        except Exception as e:
            logger.error(f"Alert resolution failed: {e}")
            raise
    
    def close(self):
        """Clean up resources."""
        self.executor.shutdown()
    
    def _init_channels(
        self,
        config: Dict[str, Any]
    ) -> Dict[str, Callable]:
        """Initialize notification channels."""
        channels = {}
        
        # Email channel
        if 'email' in config:
            channels['email'] = self._send_email
        
        # Slack channel
        if 'slack' in config:
            channels['slack'] = self._send_slack
        
        # HTTP webhook
        if 'webhook' in config:
            channels['webhook'] = self._send_webhook
        
        return channels
    
    def _init_database(self):
        """Initialize SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alerts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        type TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        message TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        metadata TEXT NOT NULL,
                        resolved BOOLEAN DEFAULT FALSE,
                        resolution TEXT,
                        resolved_at TEXT
                    )
                    """
                )
                
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_alerts_timestamp
                    ON alerts(timestamp)
                    """
                )
            
        except Exception as e:
            logger.error(
                f"Database initialization failed: {e}"
            )
            raise
    
    async def _store_alerts(
        self,
        alerts: List[Dict[str, Any]]
    ):
        """Store alerts in database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for alert in alerts:
                    metadata = {
                        k: v for k, v in alert.items()
                        if k not in {
                            'type',
                            'severity',
                            'message',
                            'timestamp'
                        }
                    }
                    
                    cursor.execute(
                        """
                        INSERT INTO alerts
                        (type, severity, message,
                         timestamp, metadata)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            alert['type'],
                            alert['severity'],
                            alert['message'],
                            alert['timestamp'],
                            json.dumps(metadata)
                        )
                    )
            
        except Exception as e:
            logger.error(f"Alert storage failed: {e}")
            raise
    
    async def _send_notifications(
        self,
        alerts: List[Dict[str, Any]]
    ):
        """Send alert notifications."""
        try:
            for channel in self.channels.values():
                try:
                    await asyncio.to_thread(
                        channel,
                        alerts
                    )
                except Exception as e:
                    logger.error(
                        f"Notification failed for "
                        f"{channel.__name__}: {e}"
                    )
            
        except Exception as e:
            logger.error(
                f"Alert notification failed: {e}"
            )
            raise
    
    def _send_email(
        self,
        alerts: List[Dict[str, Any]]
    ):
        """Send email notifications."""
        try:
            if 'email' not in self.config:
                return
            
            config = self.config['email']
            msg = EmailMessage()
            msg['Subject'] = 'Storage System Alerts'
            msg['From'] = config['from']
            msg['To'] = config['to']
            
            # Format message
            body = "Storage System Alerts:\n\n"
            for alert in alerts:
                body += (
                    f"Type: {alert['type']}\n"
                    f"Severity: {alert['severity']}\n"
                    f"Message: {alert['message']}\n"
                    f"Time: {alert['timestamp']}\n\n"
                )
            
            msg.set_content(body)
            
            # Send email
            with smtplib.SMTP(
                config['smtp_host'],
                config['smtp_port']
            ) as server:
                if config.get('use_tls'):
                    server.starttls()
                if 'username' in config:
                    server.login(
                        config['username'],
                        config['password']
                    )
                server.send_message(msg)
            
        except Exception as e:
            logger.error(f"Email notification failed: {e}")
            raise
    
    def _send_slack(
        self,
        alerts: List[Dict[str, Any]]
    ):
        """Send Slack notifications."""
        try:
            if 'slack' not in self.config:
                return
            
            config = self.config['slack']
            webhook_url = config['webhook_url']
            
            # Format message
            blocks = []
            for alert in alerts:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Alert: {alert['type']}*\n"
                            f"Severity: {alert['severity']}\n"
                            f"Message: {alert['message']}\n"
                            f"Time: {alert['timestamp']}"
                        )
                    }
                })
            
            # Send to Slack
            response = requests.post(
                webhook_url,
                json={"blocks": blocks}
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Slack notification failed: {e}")
            raise
    
    def _send_webhook(
        self,
        alerts: List[Dict[str, Any]]
    ):
        """Send webhook notifications."""
        try:
            if 'webhook' not in self.config:
                return
            
            config = self.config['webhook']
            url = config['url']
            headers = config.get('headers', {})
            
            # Send to webhook
            response = requests.post(
                url,
                json={'alerts': alerts},
                headers=headers
            )
            response.raise_for_status()
            
        except Exception as e:
            logger.error(f"Webhook notification failed: {e}")
            raise 