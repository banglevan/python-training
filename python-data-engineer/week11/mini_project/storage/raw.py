"""
Raw zone storage implementation.
"""

from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from ..datalake.core import DataLakeZone

logger = logging.getLogger(__name__)

class RawZone(DataLakeZone):
    """Raw (Bronze) zone implementation."""
    
    def write_data(
        self,
        data: Any,
        path: str,
        format: str = "delta",
        mode: str = "append",
        **options: Dict[str, Any]
    ) -> None:
        """
        Write data to raw zone.
        
        Args:
            data: Data to write
            path: Target path
            format: Storage format
            mode: Write mode
            options: Additional options
        """
        try:
            full_path = self.get_full_path(path)
            
            data.write \
                .format(format) \
                .options(**options) \
                .mode(mode) \
                .save(full_path)
            
            logger.info(f"Wrote data to raw zone: {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to write to raw zone: {e}")
            raise
    
    def read_data(
        self,
        path: str,
        format: str = "delta",
        **options: Dict[str, Any]
    ) -> Any:
        """
        Read data from raw zone.
        
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
            logger.error(f"Failed to read from raw zone: {e}")
            raise 