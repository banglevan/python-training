"""
Base connector module.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pandas as pd
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Base data connector."""
    
    def __init__(self, config: Config):
        """Initialize connector."""
        self.config = config
    
    @abstractmethod
    def connect(self) -> None:
        """Establish connection."""
        pass
    
    @abstractmethod
    def read(self, **kwargs) -> pd.DataFrame:
        """Read data from source."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection."""
        pass
    
    def validate_config(self, required_fields: List[str]) -> bool:
        """
        Validate connector configuration.
        
        Args:
            required_fields: Required configuration fields
        
        Returns:
            Whether configuration is valid
        """
        try:
            for field in required_fields:
                if field not in self.config:
                    logger.error(f"Missing required field: {field}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False 