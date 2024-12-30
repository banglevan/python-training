"""
Database connector module.
"""

from typing import Any, Dict, Optional
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from .base import BaseConnector
from src.utils.config import Config

logger = logging.getLogger(__name__)

class DatabaseConnector(BaseConnector):
    """Database connector."""
    
    def __init__(self, config: Config):
        """Initialize connector."""
        super().__init__(config)
        self.required_fields = [
            'host', 'port', 'database',
            'username', 'password'
        ]
        self.engine = None
    
    def connect(self) -> None:
        """Establish database connection."""
        try:
            if not self.validate_config(self.required_fields):
                raise ValueError("Invalid configuration")
            
            # Create connection URL
            url = (
                f"{self.config.get('driver', 'postgresql')}://"
                f"{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}"
                f"/{self.config['database']}"
            )
            
            # Create engine
            self.engine = create_engine(url)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Connected to database: {self.config['database']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def read(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Read data from database.
        
        Args:
            query: SQL query
            params: Query parameters
            **kwargs: Additional pandas read_sql arguments
        
        Returns:
            DataFrame with query results
        """
        try:
            df = pd.read_sql(
                text(query),
                self.engine,
                params=params,
                **kwargs
            )
            
            logger.info(f"Read {len(df)} rows from query")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from database: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        try:
            if self.engine:
                self.engine.dispose()
                logger.info("Closed database connection")
                
        except Exception as e:
            logger.error(f"Failed to close database connection: {e}")
            raise 