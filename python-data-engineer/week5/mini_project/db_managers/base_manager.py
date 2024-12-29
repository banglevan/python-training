"""Base Manager for Database Operations"""

from abc import ABC, abstractmethod
import logging
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseManager(ABC):
    """Abstract base class for database managers."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize with configuration."""
        self.config = config
        self.connection = None
        self._connect()
    
    @abstractmethod
    def _connect(self) -> None:
        """Establish database connection."""
        pass
    
    @abstractmethod
    def _disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """Check database health."""
        pass
    
    def close(self) -> None:
        """Safely close connection."""
        try:
            self._disconnect()
            logger.info(f"{self.__class__.__name__} connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise 