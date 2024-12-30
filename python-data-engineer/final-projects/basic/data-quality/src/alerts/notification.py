"""
Alert notification module.
"""

from typing import Dict, Any, List
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class AlertNotifier:
    """Alert notification management."""
    
    def __init__(self, config: Config):
        """Initialize notifier."""
        self.config = config
        self.smtp_config = config.alerts['smtp']
    
    def send_alert(
        self,
        level: str,
        message: str,
        details: Dict[str, Any]
    ) -> None:
        """
        Send alert notification.
        
        Args:
            level: Alert level (critical, warning, info)
            message: Alert message
            details: Alert details
        """
        try:
            # Get recipients
            recipients = self.config.alerts['levels'][level]['notify']
            
            # Create email
            msg = MIMEMultipart()
            msg['Subject'] = f"[{level.upper()}] Data Quality Alert"
            msg['From'] = self.smtp_config['from_email']
            msg['To'] = ', '.join(recipients)
            
            # Create HTML content
            html_content = f"""
            <html>
            <body>
                <h2>Data Quality Alert</h2>
                <p><strong>Level:</strong> {level.upper()}</p>
                <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Message:</strong> {message}</p>
                
                <h3>Details:</h3>
                <ul>
                {''.join(f'<li><strong>{k}:</strong> {v}</li>' for k, v in details.items())}
                </ul>
                
                <p>Please check the quality dashboard for more information.</p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            with smtplib.SMTP(
                self.smtp_config['host'],
                self.smtp_config['port']
            ) as server:
                if self.smtp_config.get('use_tls'):
                    server.starttls()
                
                if self.smtp_config.get('username'):
                    server.login(
                        self.smtp_config['username'],
                        self.smtp_config['password']
                    )
                
                server.send_message(msg)
            
            logger.info(f"Sent {level} alert to {recipients}")
            
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
            raise
    
    def send_digest(
        self,
        alerts: List[Dict[str, Any]]
    ) -> None:
        """
        Send alert digest.
        
        Args:
            alerts: List of alerts
        """
        try:
            if not alerts:
                return
            
            # Get recipients
            recipients = self.config.alerts['digest']['notify']
            
            # Create email
            msg = MIMEMultipart()
            msg['Subject'] = "Data Quality Alert Digest"
            msg['From'] = self.smtp_config['from_email']
            msg['To'] = ', '.join(recipients)
            
            # Create HTML content
            html_content = f"""
            <html>
            <body>
                <h2>Data Quality Alert Digest</h2>
                <p><strong>Period:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
                <p><strong>Total Alerts:</strong> {len(alerts)}</p>
                
                <h3>Alert Summary:</h3>
                {''.join(
                    f'''
                    <div style="margin: 10px 0; padding: 10px; border: 1px solid #ddd;">
                        <p><strong>Level:</strong> {alert['level'].upper()}</p>
                        <p><strong>Time:</strong> {alert['timestamp']}</p>
                        <p><strong>Message:</strong> {alert['message']}</p>
                        <p><strong>Details:</strong></p>
                        <ul>
                        {''.join(
                            f'<li><strong>{k}:</strong> {v}</li>'
                            for k, v in alert['details'].items()
                        )}
                        </ul>
                    </div>
                    '''
                    for alert in alerts
                )}
                
                <p>Please check the quality dashboard for more information.</p>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_content, 'html'))
            
            # Send email
            with smtplib.SMTP(
                self.smtp_config['host'],
                self.smtp_config['port']
            ) as server:
                if self.smtp_config.get('use_tls'):
                    server.starttls()
                
                if self.smtp_config.get('username'):
                    server.login(
                        self.smtp_config['username'],
                        self.smtp_config['password']
                    )
                
                server.send_message(msg)
            
            logger.info(f"Sent alert digest to {recipients}")
            
        except Exception as e:
            logger.error(f"Failed to send digest: {e}")
            raise 