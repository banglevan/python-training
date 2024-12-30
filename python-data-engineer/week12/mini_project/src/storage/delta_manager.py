"""
Delta Lake storage management.
"""

from typing import Dict, Any, Optional, List
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DeltaManager:
    """Delta Lake storage management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize manager."""
        self.spark = spark
        self.tables = {}
    
    def create_table(
        self,
        name: str,
        schema: Dict[str, str],
        partition_by: Optional[List[str]] = None,
        location: Optional[str] = None
    ) -> None:
        """
        Create Delta table.
        
        Args:
            name: Table name
            schema: Column definitions
            partition_by: Partition columns
            location: Storage location
        """
        try:
            # Build schema
            columns = [
                f"{col} {dtype}"
                for col, dtype in schema.items()
            ]
            
            # Build query
            query = f"""
                CREATE TABLE IF NOT EXISTS {name} (
                    {', '.join(columns)}
                )
                USING DELTA
            """
            
            if partition_by:
                query += f"\nPARTITIONED BY ({', '.join(partition_by)})"
            
            if location:
                query += f"\nLOCATION '{location}'"
            
            # Execute query
            self.spark.sql(query)
            
            # Store metadata
            self.tables[name] = {
                'schema': schema,
                'partition_by': partition_by,
                'location': location
            }
            
            logger.info(f"Created table: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def optimize_table(
        self,
        name: str,
        zorder_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize Delta table.
        
        Args:
            name: Table name
            zorder_by: Z-order columns
        """
        try:
            if zorder_by:
                self.spark.sql(f"""
                    OPTIMIZE {name}
                    ZORDER BY ({', '.join(zorder_by)})
                """)
            else:
                self.spark.sql(f"OPTIMIZE {name}")
            
            logger.info(f"Optimized table: {name}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    def vacuum_table(
        self,
        name: str,
        retention_hours: int = 168
    ) -> None:
        """
        Vacuum Delta table.
        
        Args:
            name: Table name
            retention_hours: Retention period
        """
        try:
            self.spark.sql(f"""
                VACUUM {name}
                RETAIN {retention_hours} HOURS
            """)
            
            logger.info(f"Vacuumed table: {name}")
            
        except Exception as e:
            logger.error(f"Failed to vacuum table: {e}")
            raise
    
    def get_table_stats(
        self,
        name: str
    ) -> Dict[str, Any]:
        """
        Get table statistics.
        
        Args:
            name: Table name
        
        Returns:
            Table statistics
        """
        try:
            # Get Delta table
            delta_table = DeltaTable.forName(self.spark, name)
            
            # Get history
            history = delta_table.history()
            
            # Get metrics
            metrics = self.spark.sql(f"""
                DESCRIBE DETAIL {name}
            """).collect()[0]
            
            return {
                'num_files': metrics.numFiles,
                'size_bytes': metrics.sizeInBytes,
                'num_partitions': metrics.numPartitions,
                'last_modified': history.first().timestamp
            }
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            raise 