"""
Curated zone storage implementation.
"""

from typing import Dict, Any, Optional, List
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from ..datalake.core import DataLakeZone

logger = logging.getLogger(__name__)

class CuratedZone(DataLakeZone):
    """Curated (Gold) zone implementation."""
    
    def write_data(
        self,
        data: Any,
        path: str,
        format: str = "delta",
        mode: str = "append",
        **options: Dict[str, Any]
    ) -> None:
        """
        Write data to curated zone.
        
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
            
            # Add optimization
            if format == "delta":
                options.update({
                    "delta.autoOptimize.optimizeWrite": "true",
                    "delta.autoOptimize.autoCompact": "true"
                })
            
            data.write \
                .format(format) \
                .options(**options) \
                .mode(mode) \
                .save(full_path)
            
            logger.info(f"Wrote data to curated zone: {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to write to curated zone: {e}")
            raise
    
    def read_data(
        self,
        path: str,
        format: str = "delta",
        **options: Dict[str, Any]
    ) -> Any:
        """
        Read data from curated zone.
        
        Args:
            path: Source path
            format: Storage format
            options: Additional options
        
        Returns:
            DataFrame
        """
        try:
            full_path = self.get_full_path(path)
            
            # Add optimization
            if format == "delta":
                options.update({
                    "delta.columnPruning.enabled": "true",
                    "delta.stats.collect": "true"
                })
            
            return self.spark.read \
                .format(format) \
                .options(**options) \
                .load(full_path)
            
        except Exception as e:
            logger.error(f"Failed to read from curated zone: {e}")
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
            
            # Check for data consistency
            # Add specific business rules here
            
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False
    
    def optimize_table(
        self,
        path: str,
        zorder_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize table in curated zone.
        
        Args:
            path: Table path
            zorder_by: Columns for Z-ordering
        """
        try:
            full_path = self.get_full_path(path)
            
            if zorder_by:
                self.spark.sql(f"""
                    OPTIMIZE '{full_path}'
                    ZORDER BY ({','.join(zorder_by)})
                """)
            else:
                self.spark.sql(f"""
                    OPTIMIZE '{full_path}'
                """)
            
            logger.info(f"Optimized table: {full_path}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise 