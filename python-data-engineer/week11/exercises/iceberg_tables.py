"""
Apache Iceberg tables exercise.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import List, Dict, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IcebergTables:
    """Apache Iceberg table operations."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize Iceberg operations."""
        self.spark = spark or SparkSession.builder \
            .appName("IcebergTables") \
            .config("spark.jars.packages", 
                   "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1") \
            .config("spark.sql.extensions", 
                   "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog", 
                   "org.apache.iceberg.spark.SparkSessionCatalog") \
            .config("spark.sql.catalog.local", 
                   "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", "warehouse") \
            .getOrCreate()
        
        logger.info("Iceberg session initialized")
    
    def create_table(
        self,
        table_name: str,
        schema: StructType,
        partition_by: List[str] = None,
        properties: Dict[str, str] = None
    ) -> None:
        """
        Create new Iceberg table.
        
        Args:
            table_name: Name of the table
            schema: Table schema
            partition_by: Partition columns
            properties: Table properties
        """
        try:
            # Create DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Build CREATE TABLE statement
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING iceberg
            """
            
            if partition_by:
                create_sql += f" PARTITIONED BY ({','.join(partition_by)})"
            
            if properties:
                props = [f"'{k}' = '{v}'" for k, v in properties.items()]
                create_sql += f" TBLPROPERTIES ({','.join(props)})"
            
            # Execute creation
            self.spark.sql(create_sql)
            logger.info(f"Created Iceberg table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def write_data(
        self,
        table_name: str,
        data: DataFrame,
        mode: str = "append"
    ) -> None:
        """
        Write data to Iceberg table.
        
        Args:
            table_name: Name of the table
            data: Data to write
            mode: Write mode (append/overwrite)
        """
        try:
            data.writeTo(table_name) \
                .using("iceberg") \
                .mode(mode) \
                .append()
            
            logger.info(f"Wrote data to {table_name} in {mode} mode")
            
        except Exception as e:
            logger.error(f"Failed to write data: {e}")
            raise
    
    def optimize_table(
        self,
        table_name: str,
        sort_by: List[str] = None
    ) -> None:
        """
        Optimize Iceberg table.
        
        Args:
            table_name: Name of the table
            sort_by: Columns to sort by
        """
        try:
            if sort_by:
                self.spark.sql(f"""
                    CALL local.system.rewrite_data_files(
                        table => '{table_name}',
                        options => map(
                            'sort-by', '{",".join(sort_by)}'
                        )
                    )
                """)
            else:
                self.spark.sql(f"""
                    CALL local.system.rewrite_data_files(
                        table => '{table_name}'
                    )
                """)
            
            logger.info(f"Optimized table: {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            raise
    
    def expire_snapshots(
        self,
        table_name: str,
        retention_hours: int = 168
    ) -> None:
        """
        Expire old snapshots.
        
        Args:
            table_name: Name of the table
            retention_hours: Retention period in hours
        """
        try:
            self.spark.sql(f"""
                CALL local.system.expire_snapshots(
                    table => '{table_name}',
                    older_than => TIMESTAMP '{datetime.now()}'
                        - INTERVAL {retention_hours} HOURS,
                    retain_last => 1
                )
            """)
            
            logger.info(f"Expired snapshots for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to expire snapshots: {e}")
            raise
    
    def analyze_table(
        self,
        table_name: str
    ) -> Dict:
        """
        Analyze table statistics.
        
        Args:
            table_name: Name of the table
        
        Returns:
            Table statistics
        """
        try:
            stats = self.spark.sql(f"""
                DESCRIBE TABLE EXTENDED {table_name}
            """).collect()
            
            # Parse statistics
            result = {}
            for row in stats:
                if row['col_name'] and row['data_type']:
                    result[row['col_name']] = row['data_type']
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to analyze table: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    iceberg = IcebergTables()
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("date", DateType(), True)
    ])
    
    try:
        # Create table
        iceberg.create_table(
            "local.db.example",
            schema,
            partition_by=["date"],
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy"
            }
        )
        
        # Generate sample data
        data = iceberg.spark.createDataFrame([
            (1, "A", 10.0, datetime.now().date()),
            (2, "B", 20.0, datetime.now().date())
        ], schema)
        
        # Write data
        iceberg.write_data(
            "local.db.example",
            data
        )
        
        # Optimize table
        iceberg.optimize_table(
            "local.db.example",
            sort_by=["id"]
        )
        
        # Expire old snapshots
        iceberg.expire_snapshots(
            "local.db.example",
            retention_hours=24
        )
        
        # Get statistics
        stats = iceberg.analyze_table("local.db.example")
        print("Table Statistics:", stats)
        
    finally:
        iceberg.spark.stop() 