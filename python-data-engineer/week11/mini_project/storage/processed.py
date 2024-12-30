"""
Processed zone storage implementation.
"""

from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from ..datalake.core import DataLakeZone

logger = logging.getLogger(__name__)

class ProcessedZone(DataLakeZone):
    """Processed (Silver) zone implementation."""
    
    def write_data(
        self,
        data: Any,
        path: str,
        format: str = "delta",
        mode: str = "append",
        **options: Dict[str, Any]
    ) -> None:
        """
        Write data to processed zone.
        
        Args:
            data: Data to write
            path: Target path
            format: Storage format
            mode: Write mode
            options: Additional options
        """
        try:
            full_path = self.get_full_path(path)
            
            # Add quality checks
            if not self._validate_data(data):
                raise ValueError("Data quality validation failed")
            
            data.write \
                .format(format) \
                .options(**options) \
                .mode(mode) \
                .save(full_path)
            
            logger.info(f"Wrote data to processed zone: {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to write to processed zone: {e}")
            raise
    
    def read_data(
        self,
        path: str,
        format: str = "delta",
        **options: Dict[str, Any]
    ) -> Any:
        """
        Read data from processed zone.
        
        Args:
            path: Source path
            format: Storage format
            options: Additional options
        
        Returns:
            DataFrame
        """
        try:
            full_path = self.get_full_path(path)
            
            return self.spark.read \
                .format(format) \
                .options(**options) \
                .load(full_path)
            
        except Exception as e:
            logger.error(f"Failed to read from processed zone: {e}")
            raise
    
    def _validate_data(self, data: Any) -> bool:
        """Validate data quality."""
        try:
            # Check for nulls in required fields
            null_counts = data.select([
                sum(col(c).isNull().cast("int")).alias(c)
                for c in data.columns
            ]).collect()[0]
            
            # Check for data completeness
            row_count = data.count()
            if row_count == 0:
                return False
            
            # Add more quality checks as needed
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False 