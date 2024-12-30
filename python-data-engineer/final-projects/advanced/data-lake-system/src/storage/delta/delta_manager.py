"""
Delta Lake table management.
"""

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from ..encryption.encryption_manager import EncryptionManager

logger = logging.getLogger(__name__)

class DeltaManager:
    """Manage Delta Lake tables."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize Delta manager.
        
        Args:
            spark: Spark session
            config: Optional configuration
        """
        self.spark = spark
        self.config = config or {}
        self.encryption_manager = EncryptionManager()
    
    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = 'append',
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: Source DataFrame
            table_name: Target table name
            mode: Write mode (append/overwrite/merge)
            partition_by: Partition columns
            options: Additional options
        """
        try:
            logger.info(f"Writing to table {table_name}")
            
            # Apply encryption if configured
            if self.config.get('enable_encryption', False):
                df = self.encryption_manager.encrypt_columns(
                    df, self.config.get('encrypted_columns', [])
                )
            
            write_options = {
                'mergeSchema': 'true',
                **(options or {})
            }
            
            writer = df.write.format('delta').mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            for k, v in write_options.items():
                writer = writer.option(k, v)
            
            writer.saveAsTable(table_name)
            
            logger.info(f"Successfully wrote to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write table: {e}")
            raise
    
    def merge_table(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_condition: str,
        when_matched: Optional[Dict[str, Any]] = None,
        when_not_matched: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Merge data into Delta table.
        
        Args:
            source_df: Source DataFrame
            target_table: Target table name
            merge_condition: Merge condition
            when_matched: Matched actions
            when_not_matched: Not matched actions
        """
        try:
            logger.info(f"Merging into table {target_table}")
            
            delta_table = DeltaTable.forName(
                self.spark, target_table
            )
            
            merge_builder = delta_table.merge(
                source_df, merge_condition
            )
            
            if when_matched:
                merge_builder = merge_builder.whenMatchedUpdate(
                    set=when_matched
                )
            
            if when_not_matched:
                merge_builder = merge_builder.whenNotMatchedInsert(
                    values=when_not_matched
                )
            
            merge_builder.execute()
            
            logger.info(f"Successfully merged into {target_table}")
            
        except Exception as e:
            logger.error(f"Failed to merge table: {e}")
            raise
    
    def vacuum_table(
        self,
        table_name: str,
        retention_hours: Optional[int] = None
    ) -> None:
        """
        Vacuum Delta table.
        
        Args:
            table_name: Table name
            retention_hours: Retention period in hours
        """
        try:
            logger.info(f"Vacuuming table {table_name}")
            
            delta_table = DeltaTable.forName(
                self.spark, table_name
            )
            
            if retention_hours:
                delta_table.vacuum(retention_hours)
            else:
                delta_table.vacuum()
            
            logger.info(f"Successfully vacuumed {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to vacuum table: {e}")
            raise
    
    def time_travel(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[datetime] = None
    ) -> DataFrame:
        """
        Query table at specific version/time.
        
        Args:
            table_name: Table name
            version: Table version
            timestamp: Timestamp
            
        Returns:
            DataFrame at specified version/time
        """
        try:
            if version is not None:
                df = self.spark.read.format('delta').option(
                    'versionAsOf', version
                ).table(table_name)
            elif timestamp is not None:
                df = self.spark.read.format('delta').option(
                    'timestampAsOf', timestamp.isoformat()
                ).table(table_name)
            else:
                raise ValueError(
                    "Must specify either version or timestamp"
                )
            
            # Decrypt if needed
            if self.config.get('enable_encryption', False):
                df = self.encryption_manager.decrypt_columns(
                    df, self.config.get('encrypted_columns', [])
                )
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to time travel: {e}")
            raise 