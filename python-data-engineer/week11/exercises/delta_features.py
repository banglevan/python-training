"""
Delta Lake features exercise.
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

class DeltaFeatures:
    """Delta Lake advanced features."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Delta features."""
        self.spark = spark or SparkSession.builder \
            .appName("DeltaFeatures") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        logger.info("Delta Lake session initialized")
    
    def enforce_schema(
        self,
        table_path: str,
        schema: StructType,
        mode: str = "append"
    ) -> None:
        """
        Enforce schema on Delta table.
        
        Args:
            table_path: Path to Delta table
            schema: Required schema
            mode: Schema enforcement mode
        """
        try:
            self.spark.sql(f"""
                ALTER TABLE '{table_path}'
                SET TBLPROPERTIES (
                    'delta.columnMapping.mode' = '{mode}',
                    'delta.schema.autoMerge.enabled' = 'true'
                )
            """)
            
            logger.info(f"Enforced schema on {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to enforce schema: {e}")
            raise
    
    def merge_data(
        self,
        table_path: str,
        source_df: DataFrame,
        merge_condition: str,
        when_matched: Optional[str] = None,
        when_not_matched: Optional[str] = None
    ) -> None:
        """
        Perform MERGE operation.
        
        Args:
            table_path: Path to Delta table
            source_df: Source data
            merge_condition: Merge condition
            when_matched: Matched condition actions
            when_not_matched: Not matched condition actions
        """
        try:
            # Create temp view for source data
            view_name = f"source_view_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            source_df.createOrReplaceTempView(view_name)
            
            # Build MERGE statement
            merge_sql = f"""
                MERGE INTO '{table_path}' target
                USING {view_name} source
                ON {merge_condition}
            """
            
            if when_matched:
                merge_sql += f" WHEN MATCHED THEN {when_matched}"
            
            if when_not_matched:
                merge_sql += f" WHEN NOT MATCHED THEN {when_not_matched}"
            
            # Execute MERGE
            self.spark.sql(merge_sql)
            
            logger.info(f"Merged data into {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to merge data: {e}")
            raise
        
        finally:
            # Clean up temp view
            self.spark.catalog.dropTempView(view_name)
    
    def setup_streaming(
        self,
        table_path: str,
        checkpoint_path: str
    ) -> None:
        """
        Setup streaming support.
        
        Args:
            table_path: Path to Delta table
            checkpoint_path: Path for checkpointing
        """
        try:
            self.spark.sql(f"""
                ALTER TABLE '{table_path}'
                SET TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.minReaderVersion' = '2',
                    'delta.minWriterVersion' = '5'
                )
            """)
            
            logger.info(f"Setup streaming for {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to setup streaming: {e}")
            raise
    
    def write_stream(
        self,
        table_path: str,
        source_df: DataFrame,
        checkpoint_path: str,
        trigger_interval: str = "5 minutes"
    ) -> None:
        """
        Write streaming data.
        
        Args:
            table_path: Path to Delta table
            source_df: Streaming source DataFrame
            checkpoint_path: Path for checkpointing
            trigger_interval: Trigger interval
        """
        try:
            stream_writer = source_df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime=trigger_interval)
            
            # Start streaming
            query = stream_writer.start(table_path)
            
            logger.info(f"Started streaming to {table_path}")
            
            return query
            
        except Exception as e:
            logger.error(f"Failed to write stream: {e}")
            raise
    
    def read_stream(
        self,
        table_path: str,
        starting_version: Optional[int] = None
    ) -> DataFrame:
        """
        Read streaming data.
        
        Args:
            table_path: Path to Delta table
            starting_version: Starting version
        
        Returns:
            Streaming DataFrame
        """
        try:
            stream_reader = self.spark.readStream \
                .format("delta") \
                .option("readChangeFeed", "true")
            
            if starting_version is not None:
                stream_reader = stream_reader.option(
                    "startingVersion",
                    starting_version
                )
            
            return stream_reader.load(table_path)
            
        except Exception as e:
            logger.error(f"Failed to read stream: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    delta_features = DeltaFeatures()
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    try:
        # Create table with schema enforcement
        delta_features.enforce_schema(
            "data/delta/features",
            schema
        )
        
        # Generate sample update data
        updates = delta_features.spark.createDataFrame([
            (1, "A_updated", 15.0, datetime.now()),
            (3, "C_new", 30.0, datetime.now())
        ], schema)
        
        # Perform MERGE operation
        delta_features.merge_data(
            "data/delta/features",
            updates,
            "target.id = source.id",
            "UPDATE SET *",
            "INSERT *"
        )
        
        # Setup streaming
        delta_features.setup_streaming(
            "data/delta/features",
            "data/delta/checkpoints"
        )
        
        # Create streaming source
        streaming_source = delta_features.spark \
            .readStream \
            .format("rate") \
            .option("rowsPerSecond", 1) \
            .load()
        
        # Start streaming write
        query = delta_features.write_stream(
            "data/delta/features",
            streaming_source,
            "data/delta/checkpoints"
        )
        
        # Wait for some data
        query.awaitTermination(60)
        
    finally:
        delta_features.spark.stop() 