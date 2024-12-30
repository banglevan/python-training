"""
Base report template module.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class BaseTemplate(ABC):
    """Base report template."""
    
    def __init__(self, config: Config):
        """Initialize template."""
        self.config = config
        self.sections = []
        self.data = {}
    
    @abstractmethod
    def add_section(
        self,
        title: str,
        content: Any,
        type: str = 'text'
    ) -> None:
        """Add section to report."""
        pass
    
    @abstractmethod
    def add_chart(
        self,
        title: str,
        data: pd.DataFrame,
        chart_type: str,
        **kwargs
    ) -> None:
        """Add chart to report."""
        pass
    
    @abstractmethod
    def add_table(
        self,
        title: str,
        data: pd.DataFrame,
        **kwargs
    ) -> None:
        """Add table to report."""
        pass
    
    @abstractmethod
    def render(self, output_path: str) -> None:
        """Render report to file."""
        pass 