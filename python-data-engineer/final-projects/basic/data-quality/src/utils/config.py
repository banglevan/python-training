"""
Configuration management module.
"""

import logging
import yaml
import os
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

class Config:
    """Configuration management."""
    
    def __init__(self, config_dir: str = "config"):
        """Initialize configuration."""
        self.config_dir = Path(config_dir)
        self.settings = {}
        self._load_configs()
    
    def _load_configs(self):
        """Load all configuration files."""
        try:
            # Load datasets config
            with open(self.config_dir / "datasets.yml", "r") as f:
                self.settings['datasets'] = yaml.safe_load(f)
            
            # Load metrics config
            with open(self.config_dir / "metrics.yml", "r") as f:
                self.settings['metrics'] = yaml.safe_load(f)
            
            # Load alerts config
            with open(self.config_dir / "alerts.yml", "r") as f:
                self.settings['alerts'] = yaml.safe_load(f)
            
            # Override with environment variables
            self._override_from_env()
            
            logger.info("Loaded configuration files")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _override_from_env(self):
        """Override settings from environment variables."""
        # SMTP settings
        self.settings['alerts']['smtp'] = {
            'host': os.getenv('SMTP_HOST', 'localhost'),
            'port': int(os.getenv('SMTP_PORT', '587')),
            'username': os.getenv('SMTP_USERNAME'),
            'password': os.getenv('SMTP_PASSWORD'),
            'from_email': os.getenv('SMTP_FROM', 'alerts@company.com'),
            'use_tls': os.getenv('SMTP_TLS', 'true').lower() == 'true'
        }
        
        # Database settings
        for db in self.settings.get('datasets', {}).values():
            if 'source' in db and 'password' in db['source']:
                db_name = db['source']['name'].upper()
                db['source']['password'] = os.getenv(
                    f'{db_name}_PASSWORD',
                    db['source'].get('password')
                )
    
    def __getattr__(self, name: str) -> Any:
        """Get configuration section."""
        if name in self.settings:
            return self.settings[name]
        raise AttributeError(f"No such configuration section: {name}") 