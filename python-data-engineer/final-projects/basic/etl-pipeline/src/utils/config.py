"""
Configuration management module.
"""

import logging
import yaml
import os
from typing import Dict, Any
from pathlib import Path

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
            # Load sources config
            with open(self.config_dir / "sources.yml", "r") as f:
                self.settings.update(yaml.safe_load(f))
            
            # Load validations
            with open(self.config_dir / "validations.yml", "r") as f:
                self.settings['validations'] = yaml.safe_load(f)
            
            # Load schedule
            with open(self.config_dir / "schedule.yml", "r") as f:
                self.settings['schedule'] = yaml.safe_load(f)
            
            # Override with environment variables
            self._override_from_env()
            
            logger.info("Loaded configuration files")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _override_from_env(self):
        """Override settings from environment variables."""
        # API keys
        self.settings['apis']['weather']['key'] = os.getenv(
            'WEATHER_API_KEY',
            self.settings['apis']['weather'].get('key')
        )
        
        self.settings['apis']['stocks']['key'] = os.getenv(
            'ALPHA_VANTAGE_KEY',
            self.settings['apis']['stocks'].get('key')
        )
        
        # Database credentials
        for db in ['mysql', 'postgres', 'warehouse']:
            if db in self.settings.get('databases', {}):
                self.settings['databases'][db]['password'] = os.getenv(
                    f'{db.upper()}_PASSWORD',
                    self.settings['databases'][db].get('password')
                )
    
    def __getattr__(self, name: str) -> Any:
        """Get configuration section."""
        if name in self.settings:
            return self.settings[name]
        raise AttributeError(f"No such configuration section: {name}") 