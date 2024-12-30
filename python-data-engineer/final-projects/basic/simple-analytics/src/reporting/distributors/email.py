"""
Email distributor module.
"""

from typing import Dict, Any, List, Optional
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class EmailDistributor:
    """Email report distributor."""
    
    def __init__(self, config: Config):
        """Initialize distributor."""
        self.config = config
        self.smtp_config = config.reports['distribution']['email']
    
    def send_report(
        self,
        recipients: List[str],
        subject: str,
        body: str,
        attachments: Optional[List[str]] = None
    ) -> None:
        """
        Send report via email.
        
        Args:
            recipients: List of email recipients
            subject: Email subject
            body: Email body
            attachments: List of file paths to attach
        """
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['from_email']
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            
            # Add body
            msg.attach(MIMEText(body, 'html'))
            
            # Add attachments
            if attachments:
                for file_path in attachments:
                    with open(file_path, 'rb') as f:
                        part = MIMEApplication(f.read())
                        part.add_header(
                            'Content-Disposition',
                            'attachment',
                            filename=Path(file_path).name
                        )
                        msg.attach(part)
            
            # Send email
            with smtplib.SMTP(
                self.smtp_config['smtp_host'],
                self.smtp_config['smtp_port']
            ) as server:
                server.starttls()
                server.login(
                    self.smtp_config['username'],
                    self.smtp_config['password']
                )
                server.send_message(msg)
            
            logger.info(f"Sent report to {len(recipients)} recipients")
            
        except Exception as e:
            logger.error(f"Failed to send report: {e}")
            raise 