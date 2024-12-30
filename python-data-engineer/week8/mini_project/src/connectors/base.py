"""
Base connector interface for data sources.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Iterator
from datetime import datetime
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SourceEvent:
    """Event structure from data sources."""
    event_id: str
    source_type: str
    source_id: str
    event_type: str
    data: Dict[str, Any]
    timestamp: datetime
    status: str = 'pending'
    checksum: Optional[str] = None

class DataConnector(ABC):
    """Abstract base class for data connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize connector with config."""
        self.config = config
        self.name = config.get('name', 'unnamed-connector')
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize connection and resources."""
        pass
    
    @abstractmethod
    def start(self) -> bool:
        """Start the connector."""
        pass
    
    @abstractmethod
    def stop(self) -> bool:
        """Stop the connector."""
        pass
    
    @abstractmethod
    def get_events(self) -> Iterator[SourceEvent]:
        """Get events from source."""
        pass
    
    @abstractmethod
    def acknowledge_event(self, event_id: str) -> bool:
        """Acknowledge event processing."""
        pass
    
    @abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """Check connector health."""
        pass 