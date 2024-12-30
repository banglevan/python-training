"""
Delta Lake table definitions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DeltaTableManager:
    """Delta Lake table manager."""
    
    def __init__(self, spark: SparkSession):
        """Initialize manager."""
        self.spark = spark
    
    def create_tables(self):
        """Create Delta tables."""
        try:
            # Sales metrics table
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS metrics_sales (
                    window struct<start: timestamp, end: timestamp>,
                    product_id string,
                    total_quantity bigint,
                    total_revenue double,
                    num_purchases bigint
                ) USING DELTA
                PARTITIONED BY (date(window.start))
            """)
            
            # User metrics table
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS metrics_users (
                    window struct<start: timestamp, end: timestamp>,
                    user_id int,
                    num_sessions bigint,
                    products_viewed bigint,
                    num_purchases bigint
                ) USING DELTA
                PARTITIONED BY (date(window.start))
            """)
            
            # Product metrics table
            self.spark.sql("""
                CREATE TABLE IF NOT EXISTS metrics_products (
                    window struct<start: timestamp, end: timestamp>,
                    product_id string,
                    views bigint,
                    cart_adds bigint,
                    purchases bigint
                ) USING DELTA
                PARTITIONED BY (date(window.start))
            """)
            
            logger.info("Created Delta tables")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def optimize_tables(self):
        """Optimize Delta tables."""
        try:
            tables = [
                "metrics_sales",
                "metrics_users",
                "metrics_products"
            ]
            
            for table in tables:
                # Optimize table
                self.spark.sql(f"OPTIMIZE {table}")
                
                # Vacuum old files
                self.spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
                
            logger.info("Optimized Delta tables")
            
        except Exception as e:
            logger.error(f"Failed to optimize tables: {e}")
            raise

if __name__ == "__main__":
    # Create Spark session
    spark = (
        SparkSession.builder
            .appName("DeltaTableManager")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )
    
    # Initialize tables
    manager = DeltaTableManager(spark)
    manager.create_tables() 