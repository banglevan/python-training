"""
Query optimization components for Data Lake Platform.
"""

from typing import Dict, Any, Optional, List
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Query optimization management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize optimizer."""
        self.spark = spark
    
    def optimize_table(
        self,
        table_path: str,
        zorder_by: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize Delta table.
        
        Args:
            table_path: Path to table
            zorder_by: Columns for Z-ordering
            partition_by: Partition columns
        """
        try:
            # Collect statistics
            self.spark.sql(f"""
                ANALYZE TABLE delta.`{table_path}`
                COMPUTE STATISTICS FOR ALL COLUMNS
            """)
            
            # Optimize and Z-order
            if zorder_by:
                self.spark.sql(f"""
                    OPTIMIZE delta.`{table_path}`
                    ZORDER BY ({','.join(zorder_by)})
                """)
            else:
                self.spark.sql(f"""
                    OPTIMIZE delta.`{table_path}`
                """)
            
            logger.info(f"Optimized table: {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    def cache_table(
        self,
        table_path: str,
        cache_name: str
    ) -> None:
        """
        Cache frequently accessed data.
        
        Args:
            table_path: Path to table
            cache_name: Name for cached view
        """
        try:
            df = self.spark.read.format("delta").load(table_path)
            df.createOrReplaceTempView(cache_name)
            
            self.spark.sql(f"""
                CACHE TABLE {cache_name}
            """)
            
            logger.info(f"Cached table as: {cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to cache table: {e}")
            raise
    
    def set_table_properties(
        self,
        table_path: str,
        properties: Dict[str, str]
    ) -> None:
        """
        Set optimization properties.
        
        Args:
            table_path: Path to table
            properties: Properties to set
        """
        try:
            table = DeltaTable.forPath(self.spark, table_path)
            
            # Set properties
            for key, value in properties.items():
                table.alterTable() \
                    .setProperty(key, value) \
                    .execute()
            
            logger.info(f"Set properties for: {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to set properties: {e}")
            raise
    
    def get_table_stats(
        self,
        table_path: str
    ) -> Dict[str, Any]:
        """
        Get table statistics.
        
        Args:
            table_path: Path to table
        
        Returns:
            Statistics dictionary
        """
        try:
            stats = self.spark.sql(f"""
                DESCRIBE DETAIL delta.`{table_path}`
            """).collect()[0]
            
            return {
                'num_files': stats.numFiles,
                'size_bytes': stats.sizeInBytes,
                'partition_columns': stats.partitionColumns,
                'z_ordered_by': stats.zOrderedBy
            }
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            raise
    
    def suggest_optimizations(
        self,
        table_path: str
    ) -> List[str]:
        """
        Suggest optimization strategies.
        
        Args:
            table_path: Path to table
        
        Returns:
            List of suggestions
        """
        try:
            suggestions = []
            stats = self.get_table_stats(table_path)
            
            # Check file size
            if stats['size_bytes'] > 1024 * 1024 * 1024:  # 1GB
                suggestions.append("Consider partitioning data")
            
            # Check number of files
            if stats['num_files'] > 1000:
                suggestions.append("Consider compacting small files")
            
            # Check Z-ordering
            if not stats['z_ordered_by']:
                suggestions.append("Consider Z-ordering for frequently filtered columns")
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Failed to generate suggestions: {e}")
            raise 