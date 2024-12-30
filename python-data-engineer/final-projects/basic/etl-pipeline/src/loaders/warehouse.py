"""
Data warehouse loading module.
"""

import logging
from typing import Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from src.utils.config import Config
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

class WarehouseLoader:
    """Data warehouse loading management."""
    
    def __init__(self, config: Config):
        """Initialize loader."""
        self.config = config
        self._init_connection()
    
    def _init_connection(self):
        """Initialize warehouse connection."""
        try:
            # Create connection
            warehouse_config = self.config.warehouse
            url = (
                f"postgresql://{warehouse_config['user']}:{warehouse_config['password']}"
                f"@{warehouse_config['host']}:{warehouse_config['port']}"
                f"/{warehouse_config['database']}"
            )
            self.engine = create_engine(url)
            
            logger.info("Initialized warehouse connection")
            
        except Exception as e:
            logger.error(f"Failed to initialize warehouse connection: {e}")
            raise
    
    def load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: list
    ) -> None:
        """
        Load dimension table.
        
        Args:
            df: Data DataFrame
            table_name: Target table name
            key_columns: Key columns for updates
        """
        try:
            # Add metadata columns
            df['dw_loaded_at'] = datetime.now()
            
            # Upsert data
            with self.engine.begin() as conn:
                # Create temp table
                temp_table = f"temp_{table_name}"
                df.to_sql(
                    temp_table,
                    conn,
                    if_exists='replace',
                    index=False
                )
                
                # Perform upsert
                key_conditions = " AND ".join([
                    f"target.{col} = source.{col}"
                    for col in key_columns
                ])
                
                set_conditions = ", ".join([
                    f"{col} = source.{col}"
                    for col in df.columns
                    if col not in key_columns
                ])
                
                upsert_query = f"""
                    INSERT INTO {table_name} AS target
                    SELECT * FROM {temp_table} AS source
                    ON CONFLICT ({', '.join(key_columns)})
                    DO UPDATE SET
                        {set_conditions},
                        dw_updated_at = NOW()
                """
                
                conn.execute(text(upsert_query))
                
                # Drop temp table
                conn.execute(text(f"DROP TABLE {temp_table}"))
            
            logger.info(f"Loaded {len(df)} records into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load dimension table: {e}")
            raise
    
    def load_fact(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> None:
        """
        Load fact table.
        
        Args:
            df: Data DataFrame
            table_name: Target table name
        """
        try:
            # Add metadata columns
            df['dw_loaded_at'] = datetime.now()
            
            # Append data
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Loaded {len(df)} records into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load fact table: {e}")
            raise
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query.
        
        Args:
            query: SQL query
        
        Returns:
            Query results DataFrame
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    def close(self):
        """Close warehouse connection."""
        self.engine.dispose()
        logger.info("Closed warehouse connection") 