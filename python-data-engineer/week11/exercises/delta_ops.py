"""
Delta Lake operations exercise.
"""

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DeltaOperations:
    """Delta Lake table management and operations."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Delta operations."""
        self.spark = spark or SparkSession.builder \
            .appName("DeltaOperations") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        logger.info("Delta Lake session initialized")
    
    def create_table(
        self,
        table_path: str,
        schema: StructType,
        partition_cols: List[str] = None
    ) -> None:
        """
        Create new Delta table.
        
        Args:
            table_path: Path to Delta table
            schema: Table schema
            partition_cols: Partition columns
        """
        try:
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write as Delta format
            writer = empty_df.write.format("delta")
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(table_path)
            logger.info(f"Created Delta table at {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to create Delta table: {e}")
            raise
    
    def write_data(
        self,
        table_path: str,
        data: DataFrame,
        mode: str = "append"
    ) -> None:
        """
        Write data to Delta table.
        
        Args:
            table_path: Path to Delta table
            data: Data to write
            mode: Write mode (append/overwrite)
        """
        try:
            data.write.format("delta") \
                .mode(mode) \
                .save(table_path)
            
            logger.info(f"Wrote data to {table_path} in {mode} mode")
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    def optimize_table(
        self,
        table_path: str,
        zorder_cols: List[str] = None
    ) -> None:
        """
        Optimize Delta table.
        
        Args:
            table_path: Path to Delta table
            zorder_cols: Columns for Z-ordering
        """
        try:
            if zorder_cols:
                self.spark.sql(f"""
                    OPTIMIZE '{table_path}'
                    ZORDER BY ({','.join(zorder_cols)})
                """)
            else:
                self.spark.sql(f"OPTIMIZE '{table_path}'")
            
            logger.info(f"Optimized table at {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    def vacuum_table(
        self,
        table_path: str,
        retention_hours: int = 168
    ) -> None:
        """
        Vacuum Delta table.
        
        Args:
            table_path: Path to Delta table
            retention_hours: Retention period in hours
        """
        try:
            self.spark.sql(f"""
                VACUUM '{table_path}'
                RETAIN {retention_hours} HOURS
            """)
            
            logger.info(f"Vacuumed table at {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to vacuum table: {e}")
            raise
    
    def get_version_history(
        self,
        table_path: str,
        limit: int = 10
    ) -> List[Dict]:
        """
        Get table version history.
        
        Args:
            table_path: Path to Delta table
            limit: Maximum versions to return
        
        Returns:
            List of version details
        """
        try:
            history_df = self.spark.sql(f"""
                DESCRIBE HISTORY '{table_path}'
                LIMIT {limit}
            """)
            
            return history_df.toPandas().to_dict('records')
            
        except Exception as e:
            logger.error(f"Failed to get version history: {e}")
            raise
    
    def restore_version(
        self,
        table_path: str,
        version: int
    ) -> None:
        """
        Restore table to specific version.
        
        Args:
            table_path: Path to Delta table
            version: Version number to restore
        """
        try:
            self.spark.sql(f"""
                RESTORE TABLE '{table_path}'
                TO VERSION AS OF {version}
            """)
            
            logger.info(f"Restored table to version {version}")
            
        except Exception as e:
            logger.error(f"Failed to restore version: {e}")
            raise
    
    def compact_files(
        self,
        table_path: str,
        target_size: str = "128mb"
    ) -> None:
        """
        Compact small files.
        
        Args:
            table_path: Path to Delta table
            target_size: Target file size
        """
        try:
            self.spark.sql(f"""
                OPTIMIZE '{table_path}'
                WHERE size < {target_size}
            """)
            
            logger.info(f"Compacted files in {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to compact files: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    delta_ops = DeltaOperations()
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("date", DateType(), True)
    ])
    
    try:
        # Create table
        delta_ops.create_table(
            "data/delta/example",
            schema,
            partition_cols=["date"]
        )
        
        # Generate sample data
        data = delta_ops.spark.createDataFrame([
            (1, "A", 10.0, datetime.now().date()),
            (2, "B", 20.0, datetime.now().date())
        ], schema)
        
        # Write data
        delta_ops.write_data(
            "data/delta/example",
            data
        )
        
        # Optimize and vacuum
        delta_ops.optimize_table(
            "data/delta/example",
            zorder_cols=["id"]
        )
        
        delta_ops.vacuum_table(
            "data/delta/example",
            retention_hours=24
        )
        
        # Get history
        history = delta_ops.get_version_history(
            "data/delta/example"
        )
        print("Version History:", history)
        
    finally:
        delta_ops.spark.stop() 