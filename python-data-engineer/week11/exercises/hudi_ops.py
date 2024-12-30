"""
Apache Hudi operations exercise.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HudiOperations:
    """Apache Hudi table operations."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Hudi operations."""
        self.spark = spark or SparkSession.builder \
            .appName("HudiOperations") \
            .config("spark.jars.packages", 
                   "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.13.1") \
            .config("spark.sql.extensions",
                   "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
            .getOrCreate()
        
        logger.info("Hudi session initialized")
    
    def create_cow_table(
        self,
        table_path: str,
        table_name: str,
        schema: StructType,
        key_fields: List[str],
        partition_fields: List[str] = None,
        pre_combine_field: str = None
    ) -> None:
        """
        Create Copy-on-Write table.
        
        Args:
            table_path: Path to table
            table_name: Name of the table
            schema: Table schema
            key_fields: Record key fields
            partition_fields: Partition fields
            pre_combine_field: Field for combining records
        """
        try:
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Configure Hudi options
            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': ','.join(key_fields),
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE'
            }
            
            if partition_fields:
                hudi_options['hoodie.datasource.write.partitionpath.field'] = \
                    ','.join(partition_fields)
            
            if pre_combine_field:
                hudi_options['hoodie.datasource.write.precombine.field'] = \
                    pre_combine_field
            
            # Write empty DataFrame to initialize table
            empty_df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(table_path)
            
            logger.info(f"Created COW table at {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to create COW table: {e}")
            raise
    
    def create_mor_table(
        self,
        table_path: str,
        table_name: str,
        schema: StructType,
        key_fields: List[str],
        partition_fields: List[str] = None,
        pre_combine_field: str = None
    ) -> None:
        """
        Create Merge-on-Read table.
        
        Args:
            table_path: Path to table
            table_name: Name of the table
            schema: Table schema
            key_fields: Record key fields
            partition_fields: Partition fields
            pre_combine_field: Field for combining records
        """
        try:
            empty_df = self.spark.createDataFrame([], schema)
            
            hudi_options = {
                'hoodie.table.name': table_name,
                'hoodie.datasource.write.recordkey.field': ','.join(key_fields),
                'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
                'hoodie.compact.inline': 'true'
            }
            
            if partition_fields:
                hudi_options['hoodie.datasource.write.partitionpath.field'] = \
                    ','.join(partition_fields)
            
            if pre_combine_field:
                hudi_options['hoodie.datasource.write.precombine.field'] = \
                    pre_combine_field
            
            empty_df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(table_path)
            
            logger.info(f"Created MOR table at {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to create MOR table: {e}")
            raise
    
    def upsert_data(
        self,
        table_path: str,
        data: DataFrame,
        key_fields: List[str],
        partition_fields: List[str] = None,
        pre_combine_field: str = None
    ) -> None:
        """
        Upsert data into Hudi table.
        
        Args:
            table_path: Path to table
            data: Data to upsert
            key_fields: Record key fields
            partition_fields: Partition fields
            pre_combine_field: Field for combining records
        """
        try:
            hudi_options = {
                'hoodie.datasource.write.recordkey.field': ','.join(key_fields),
                'hoodie.datasource.write.operation': 'upsert'
            }
            
            if partition_fields:
                hudi_options['hoodie.datasource.write.partitionpath.field'] = \
                    ','.join(partition_fields)
            
            if pre_combine_field:
                hudi_options['hoodie.datasource.write.precombine.field'] = \
                    pre_combine_field
            
            data.write.format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(table_path)
            
            logger.info(f"Upserted data to {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to upsert data: {e}")
            raise
    
    def delete_data(
        self,
        table_path: str,
        condition: str
    ) -> None:
        """
        Delete data from Hudi table.
        
        Args:
            table_path: Path to table
            condition: Delete condition
        """
        try:
            self.spark.sql(f"""
                DELETE FROM {table_path}
                WHERE {condition}
            """)
            
            logger.info(f"Deleted data from {table_path}")
            
        except Exception as e:
            logger.error(f"Failed to delete data: {e}")
            raise
    
    def query_incremental(
        self,
        table_path: str,
        begin_time: str,
        end_time: Optional[str] = None
    ) -> DataFrame:
        """
        Query incremental changes.
        
        Args:
            table_path: Path to table
            begin_time: Begin timestamp
            end_time: End timestamp
        
        Returns:
            DataFrame with incremental changes
        """
        try:
            options = {
                'hoodie.datasource.query.type': 'incremental',
                'hoodie.datasource.read.begin.instanttime': begin_time
            }
            
            if end_time:
                options['hoodie.datasource.read.end.instanttime'] = end_time
            
            return self.spark.read.format("hudi") \
                .options(**options) \
                .load(table_path)
            
        except Exception as e:
            logger.error(f"Failed to query incremental changes: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    hudi = HudiOperations()
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ts", TimestampType(), True),
        StructField("date", DateType(), True)
    ])
    
    try:
        # Create COW table
        hudi.create_cow_table(
            "data/hudi/cow_example",
            "cow_example",
            schema,
            key_fields=["id"],
            partition_fields=["date"],
            pre_combine_field="ts"
        )
        
        # Create MOR table
        hudi.create_mor_table(
            "data/hudi/mor_example",
            "mor_example",
            schema,
            key_fields=["id"],
            partition_fields=["date"],
            pre_combine_field="ts"
        )
        
        # Generate sample data
        data = hudi.spark.createDataFrame([
            (1, "A", 10.0, datetime.now(), datetime.now().date()),
            (2, "B", 20.0, datetime.now(), datetime.now().date())
        ], schema)
        
        # Upsert data
        hudi.upsert_data(
            "data/hudi/cow_example",
            data,
            key_fields=["id"],
            partition_fields=["date"],
            pre_combine_field="ts"
        )
        
        # Query incremental changes
        changes = hudi.query_incremental(
            "data/hudi/cow_example",
            begin_time="000"  # From beginning
        )
        print("Incremental Changes:", changes.show())
        
    finally:
        hudi.spark.stop() 