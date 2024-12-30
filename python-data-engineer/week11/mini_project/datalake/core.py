"""
Core components for Modern Data Lake Platform.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLakeZone(ABC):
    """Abstract base class for data lake zones."""
    
    def __init__(self, spark: SparkSession, base_path: str):
        """
        Initialize data lake zone.
        
        Args:
            spark: SparkSession instance
            base_path: Base storage path
        """
        self.spark = spark
        self.base_path = base_path
        
    @abstractmethod
    def write_data(
        self,
        data: Any,
        path: str,
        format: str,
        mode: str = "append",
        **options: Dict[str, Any]
    ) -> None:
        """Write data to zone."""
        pass
    
    @abstractmethod
    def read_data(
        self,
        path: str,
        format: str,
        **options: Dict[str, Any]
    ) -> Any:
        """Read data from zone."""
        pass
    
    def get_full_path(self, path: str) -> str:
        """Get full storage path."""
        return f"{self.base_path}/{path}"

class DataLakeTable:
    """Data Lake table management."""
    
    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        schema: Optional[StructType] = None
    ):
        """
        Initialize table.
        
        Args:
            spark: SparkSession instance
            table_path: Path to table
            schema: Table schema
        """
        self.spark = spark
        self.table_path = table_path
        self.schema = schema
    
    def create_table(
        self,
        partition_by: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> None:
        """Create Delta table."""
        try:
            if not self.schema:
                raise ValueError("Schema is required for table creation")
            
            # Create empty DataFrame
            empty_df = self.spark.createDataFrame([], self.schema)
            
            # Write as Delta format
            writer = empty_df.write \
                .format("delta") \
                .mode("errorIfExists")
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            if properties:
                for key, value in properties.items():
                    writer = writer.option(key, value)
            
            writer.save(self.table_path)
            logger.info(f"Created table at {self.table_path}")
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def upsert_data(
        self,
        data: DataFrame,
        merge_key: str,
        when_matched: str = "update *",
        when_not_matched: str = "insert *"
    ) -> None:
        """
        Upsert data into table.
        
        Args:
            data: Data to upsert
            merge_key: Merge condition
            when_matched: Matched action
            when_not_matched: Not matched action
        """
        try:
            table = DeltaTable.forPath(self.spark, self.table_path)
            
            table.alias("target") \
                .merge(
                    data.alias("source"),
                    merge_key
                ) \
                .whenMatchedUpdate(
                    condition=None,
                    set=when_matched
                ) \
                .whenNotMatchedInsert(
                    condition=None,
                    values=when_not_matched
                ) \
                .execute()
            
            logger.info(f"Upserted data into {self.table_path}")
            
        except Exception as e:
            logger.error(f"Failed to upsert data: {e}")
            raise
    
    def optimize_table(
        self,
        zorder_by: Optional[List[str]] = None
    ) -> None:
        """
        Optimize table.
        
        Args:
            zorder_by: Columns for Z-ordering
        """
        try:
            if zorder_by:
                self.spark.sql(f"""
                    OPTIMIZE '{self.table_path}'
                    ZORDER BY ({','.join(zorder_by)})
                """)
            else:
                self.spark.sql(f"""
                    OPTIMIZE '{self.table_path}'
                """)
            
            logger.info(f"Optimized table {self.table_path}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    def vacuum_table(
        self,
        retention_hours: int = 168  # 7 days
    ) -> None:
        """
        Vacuum table.
        
        Args:
            retention_hours: Hours to retain
        """
        try:
            self.spark.sql(f"""
                VACUUM '{self.table_path}'
                RETAIN {retention_hours} HOURS
            """)
            
            logger.info(f"Vacuumed table {self.table_path}")
            
        except Exception as e:
            logger.error(f"Failed to vacuum table: {e}")
            raise

class MetricsCollector:
    """Collect and track metrics."""
    
    def __init__(self):
        """Initialize metrics collector."""
        self.metrics = {}
        self.start_time = None
    
    def start_operation(self, operation: str):
        """Start timing operation."""
        self.start_time = datetime.now()
        
    def end_operation(self, operation: str, success: bool = True):
        """End timing operation."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            
            if operation not in self.metrics:
                self.metrics[operation] = {
                    'count': 0,
                    'success': 0,
                    'failure': 0,
                    'total_duration': 0
                }
            
            self.metrics[operation]['count'] += 1
            self.metrics[operation]['total_duration'] += duration
            
            if success:
                self.metrics[operation]['success'] += 1
            else:
                self.metrics[operation]['failure'] += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics."""
        return self.metrics 