"""
Configuration management module.
"""

import logging
from typing import Dict, Any, Optional
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Config:
    """Application configuration manager."""
    
    def __init__(self, config_path: str):
        """Initialize configuration."""
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Configuration loading failed: {e}")
            raise
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        try:
            keys = key.split('.')
            value = self.config
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def validate(self) -> bool:
        """Validate configuration."""
        required_keys = [
            'database.postgres.host',
            'database.postgres.port',
            'database.postgres.user',
            'database.postgres.password',
            'database.postgres.dbname',
            'security.secret_key',
            'security.algorithm',
            'api.host',
            'api.port'
        ]
        
        try:
            for key in required_keys:
                if not self.get(key):
                    logger.error(f"Missing required config: {key}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

# Global configuration instance
config = Config(str(Path(__file__).parent.parent.parent / 'config' / 'config.yml')) 