"""
Database data extraction module.
"""

import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, text
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class DatabaseExtractor:
    """Database data extraction management."""
    
    def __init__(self, config: Config):
        """Initialize extractor."""
        self.config = config
        self.connections = {}
        self._init_connections()
    
    def _init_connections(self):
        """Initialize database connections."""
        try:
            # MySQL connection
            mysql_config = self.config.databases['mysql']
            mysql_url = (
                f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
                f"@{mysql_config['host']}:{mysql_config['port']}"
                f"/{mysql_config['database']}"
            )
            self.connections['mysql'] = create_engine(mysql_url)
            
            # PostgreSQL connection
            pg_config = self.config.databases['postgres']
            pg_url = (
                f"postgresql://{pg_config['user']}:{pg_config['password']}"
                f"@{pg_config['host']}:{pg_config['port']}"
                f"/{pg_config['database']}"
            )
            self.connections['postgres'] = create_engine(pg_url)
            
            logger.info("Initialized database connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize connections: {e}")
            raise
    
    def extract_customer_data(
        self,
        last_update: str = None
    ) -> pd.DataFrame:
        """
        Extract customer data from MySQL.
        
        Args:
            last_update: Last update timestamp
        
        Returns:
            Customer data DataFrame
        """
        try:
            # Build query
            query = """
                SELECT 
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    phone,
                    address,
                    created_at,
                    updated_at
                FROM customers
            """
            
            if last_update:
                query += f" WHERE updated_at > '{last_update}'"
            
            # Execute query
            df = pd.read_sql(
                query,
                self.connections['mysql']
            )
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract customer data: {e}")
            raise
    
    def extract_transaction_data(
        self,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        Extract transaction data from PostgreSQL.
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            Transaction data DataFrame
        """
        try:
            # Build query
            query = """
                SELECT 
                    transaction_id,
                    customer_id,
                    product_id,
                    amount,
                    transaction_date,
                    status
                FROM transactions
                WHERE transaction_date BETWEEN :start_date AND :end_date
            """
            
            # Execute query
            df = pd.read_sql(
                query,
                self.connections['postgres'],
                params={
                    'start_date': start_date,
                    'end_date': end_date
                }
            )
            
            logger.info(f"Extracted {len(df)} transaction records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract transaction data: {e}")
            raise
    
    def close(self):
        """Close database connections."""
        for engine in self.connections.values():
            engine.dispose()
        logger.info("Closed database connections") 