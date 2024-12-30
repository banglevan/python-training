"""
Slack distributor module.
"""

from typing import Dict, Any, List, Optional
import requests
import json
from pathlib import Path
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class SlackDistributor:
    """Slack report distributor."""
    
    def __init__(self, config: Config):
        """Initialize distributor."""
        self.config = config
        self.slack_config = config.reports['distribution']['slack']
    
    def send_report(
        self,
        channel: str,
        message: str,
        attachments: Optional[List[str]] = None,
        blocks: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """
        Send report to Slack.
        
        Args:
            channel: Slack channel
            message: Message text
            attachments: File paths to attach
            blocks: Slack blocks for rich formatting
        """
        try:
            # Prepare payload
            payload = {
                'channel': channel,
                'text': message
            }
            
            if blocks:
                payload['blocks'] = blocks
            
            # Send message
            response = requests.post(
                self.slack_config['webhook_url'],
                json=payload
            )
            response.raise_for_status()
            
            # Upload files
            if attachments:
                for file_path in attachments:
                    with open(file_path, 'rb') as f:
                        response = requests.post(
                            'https://slack.com/api/files.upload',
                            headers={
                                'Authorization': f"Bearer {self.slack_config['token']}"
                            },
                            data={
                                'channels': channel,
                                'filename': Path(file_path).name
                            },
                            files={'file': f}
                        )
                        response.raise_for_status()
            
            logger.info(f"Sent report to Slack channel: {channel}")
            
        except Exception as e:
            logger.error(f"Failed to send report to Slack: {e}")
            raise
    
    def create_report_blocks(
        self,
        title: str,
        sections: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Create Slack blocks for report.
        
        Args:
            title: Report title
            sections: Report sections
        
        Returns:
            List of Slack blocks
        """
        try:
            blocks = [{
                'type': 'header',
                'text': {
                    'type': 'plain_text',
                    'text': title
                }
            }]
            
            for section in sections:
                blocks.extend([
                    {'type': 'divider'},
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': f"*{section['title']}*\n{section['content']}"
                        }
                    }
                ])
            
            return blocks
            
        except Exception as e:
            logger.error(f"Failed to create Slack blocks: {e}")
            raise 