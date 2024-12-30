"""
CSV connector module.
"""

from typing import Any, Dict, Optional
import pandas as pd
from pathlib import Path
import logging
from .base import BaseConnector
from src.utils.config import Config

logger = logging.getLogger(__name__)

class CSVConnector(BaseConnector):
    """CSV file connector."""
    
    def __init__(self, config: Config):
        """Initialize connector."""
        super().__init__(config)
        self.required_fields = ['path', 'delimiter']
        self.path = None
    
    def connect(self) -> None:
        """Validate and set up file path."""
        try:
            if not self.validate_config(self.required_fields):
                raise ValueError("Invalid configuration")
            
            self.path = Path(self.config['path'])
            if not self.path.exists():
                raise FileNotFoundError(f"File not found: {self.path}")
                
            logger.info(f"Connected to CSV file: {self.path}")
            
        except Exception as e:
            logger.error(f"Failed to connect to CSV file: {e}")
            raise
    
    def read(
        self,
        usecols: Optional[List[str]] = None,
        dtype: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read CSV file.
        
        Args:
            usecols: Columns to read
            dtype: Column data types
            **kwargs: Additional pandas read_csv arguments
        
        Returns:
            DataFrame with CSV data
        """
        try:
            df = pd.read_csv(
                self.path,
                delimiter=self.config['delimiter'],
                usecols=usecols,
                dtype=dtype,
                **kwargs
            )
            
            logger.info(f"Read {len(df)} rows from {self.path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read CSV file: {e}")
            raise
    
    def close(self) -> None:
        """No connection to close for CSV."""
        pass 