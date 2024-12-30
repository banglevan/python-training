"""
Configuration management module.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
import os
import logging

logger = logging.getLogger(__name__)

class Config:
    """Configuration manager."""
    
    def __init__(self, config_dir: str = "config"):
        """Initialize configuration."""
        self.config_dir = Path(config_dir)
        self.settings = {}
        self._load_config()
    
    def _load_config(self):
        """Load all configuration files."""
        try:
            # Load sources config
            with open(self.config_dir / "sources.yml") as f:
                self.settings['sources'] = yaml.safe_load(f)
            
            # Load metrics config
            with open(self.config_dir / "metrics.yml") as f:
                self.settings['metrics'] = yaml.safe_load(f)
            
            # Load reports config
            with open(self.config_dir / "reports.yml") as f:
                self.settings['reports'] = yaml.safe_load(f)
            
            # Environment variables
            self._process_env_vars(self.settings)
            
            logger.info("Loaded configuration files")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _process_env_vars(self, config: Dict[str, Any]):
        """Process environment variables in config."""
        if isinstance(config, dict):
            for key, value in config.items():
                if isinstance(value, str) and value.startswith("${"):
                    env_var = value[2:-1]
                    config[key] = os.environ.get(env_var)
                elif isinstance(value, (dict, list)):
                    self._process_env_vars(value)
        elif isinstance(config, list):
            for item in config:
                if isinstance(item, (dict, list)):
                    self._process_env_vars(item)
    
    def __getattr__(self, name: str) -> Any:
        """Get configuration section."""
        if name in self.settings:
            return self.settings[name]
        raise AttributeError(f"No such configuration section: {name}") 