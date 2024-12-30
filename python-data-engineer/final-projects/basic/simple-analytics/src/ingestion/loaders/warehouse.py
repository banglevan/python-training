"""
Data warehouse loader module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
from src.utils.config import Config

logger = logging.getLogger(__name__)

class WarehouseLoader:
    """Data warehouse loader."""
    
    def __init__(self, config: Config):
        """Initialize loader."""
        self.config = config
        self.engine = None
        self._connect()
    
    def _connect(self) -> None:
        """Establish warehouse connection."""
        try:
            warehouse_config = self.config.warehouse
            
            # Create connection URL
            url = (
                f"{warehouse_config.get('driver', 'postgresql')}://"
                f"{warehouse_config['username']}:{warehouse_config['password']}"
                f"@{warehouse_config['host']}:{warehouse_config['port']}"
                f"/{warehouse_config['database']}"
            )
            
            self.engine = create_engine(url)
            logger.info("Connected to data warehouse")
            
        except Exception as e:
            logger.error(f"Failed to connect to warehouse: {e}")
            raise
    
    def load_dimension(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> None:
        """
        Load dimension table with SCD Type 2.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            key_columns: Business key columns
            update_columns: Columns to check for updates
        """
        try:
            # Add tracking columns
            df['valid_from'] = datetime.now()
            df['valid_to'] = None
            df['is_current'] = True
            
            with self.engine.begin() as conn:
                # Get existing records
                keys_str = ", ".join(key_columns)
                existing = pd.read_sql(
                    f"SELECT * FROM {table_name} WHERE is_current = true",
                    conn
                )
                
                if len(existing) > 0:
                    # Find changes
                    merged = df.merge(
                        existing,
                        on=key_columns,
                        how='outer',
                        indicator=True,
                        suffixes=('_new', '')
                    )
                    
                    # New records
                    new_records = merged[merged['_merge'] == 'left_only']
                    if len(new_records) > 0:
                        new_records = new_records[df.columns]
                        new_records.to_sql(
                            table_name,
                            conn,
                            if_exists='append',
                            index=False
                        )
                    
                    # Changed records
                    if update_columns:
                        changed = merged[merged['_merge'] == 'both']
                        for col in update_columns:
                            changed = changed[
                                changed[f"{col}_new"] != changed[col]
                            ]
                        
                        if len(changed) > 0:
                            # Expire old records
                            changed_keys = changed[key_columns].values.tolist()
                            placeholders = ','.join(['%s'] * len(key_columns))
                            update_sql = f"""
                                UPDATE {table_name}
                                SET valid_to = CURRENT_TIMESTAMP,
                                    is_current = false
                                WHERE ({keys_str}) IN ({placeholders})
                                AND is_current = true
                            """
                            conn.execute(text(update_sql), changed_keys)
                            
                            # Insert new versions
                            new_versions = changed[df.columns]
                            new_versions.to_sql(
                                table_name,
                                conn,
                                if_exists='append',
                                index=False
                            )
                else:
                    # Initial load
                    df.to_sql(
                        table_name,
                        conn,
                        if_exists='append',
                        index=False
                    )
                
            logger.info(f"Loaded dimension table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load dimension: {e}")
            raise
    
    def load_fact(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str]
    ) -> None:
        """
        Load fact table incrementally.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            key_columns: Business key columns
        """
        try:
            with self.engine.begin() as conn:
                # Get existing keys
                keys_str = ", ".join(key_columns)
                existing_keys = pd.read_sql(
                    f"SELECT DISTINCT {keys_str} FROM {table_name}",
                    conn
                )
                
                # Find new records
                if len(existing_keys) > 0:
                    merged = df.merge(
                        existing_keys,
                        on=key_columns,
                        how='left',
                        indicator=True
                    )
                    new_records = merged[merged['_merge'] == 'left_only']
                    new_records = new_records[df.columns]
                else:
                    new_records = df
                
                # Load new records
                if len(new_records) > 0:
                    new_records.to_sql(
                        table_name,
                        conn,
                        if_exists='append',
                        index=False
                    )
                
                logger.info(
                    f"Loaded {len(new_records)} new records into {table_name}"
                )
                
        except Exception as e:
            logger.error(f"Failed to load fact: {e}")
            raise
    
    def close(self) -> None:
        """Close warehouse connection."""
        try:
            if self.engine:
                self.engine.dispose()
                logger.info("Closed warehouse connection")
                
        except Exception as e:
            logger.error(f"Failed to close connection: {e}")
            raise 