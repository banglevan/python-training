"""
Spark Streaming job for processing e-commerce events.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProcessor:
    """Real-time event processor using Spark Streaming."""
    
    def __init__(self, spark: SparkSession):
        """Initialize processor."""
        self.spark = spark
        self.schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("session_id", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True)
        ])
    
    def read_stream(self) -> DataFrame:
        """Read Kafka stream."""
        return (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:29092")
                .option("subscribe", "ecommerce_events")
                .option("startingOffsets", "latest")
                .load()
                .select(from_json(col("value").cast("string"), self.schema).alias("data"))
                .select("data.*")
        )
    
    def process_events(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Process streaming events."""
        # Window for aggregations
        window_duration = "5 minutes"
        slide_duration = "1 minute"
        
        # Sales metrics
        sales_metrics = (
            df.filter(col("event_type") == "purchase")
            .withWatermark("timestamp", "10 minutes")
            .groupBy(
                window("timestamp", window_duration, slide_duration),
                "product_id"
            )
            .agg(
                sum("quantity").alias("total_quantity"),
                sum(col("price") * col("quantity")).alias("total_revenue"),
                count("event_id").alias("num_purchases")
            )
        )
        
        # User activity metrics
        user_metrics = (
            df.withWatermark("timestamp", "10 minutes")
            .groupBy(
                window("timestamp", window_duration, slide_duration),
                "user_id"
            )
            .agg(
                countDistinct("session_id").alias("num_sessions"),
                countDistinct("product_id").alias("products_viewed"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0))
                    .alias("num_purchases")
            )
        )
        
        # Product performance metrics
        product_metrics = (
            df.withWatermark("timestamp", "10 minutes")
            .groupBy(
                window("timestamp", window_duration, slide_duration),
                "product_id"
            )
            .agg(
                sum(when(col("event_type") == "view", 1).otherwise(0))
                    .alias("views"),
                sum(when(col("event_type") == "cart", 1).otherwise(0))
                    .alias("cart_adds"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0))
                    .alias("purchases")
            )
        )
        
        return {
            "sales": sales_metrics,
            "users": user_metrics,
            "products": product_metrics
        }
    
    def write_to_delta(self, df: DataFrame, table_name: str) -> StreamingQuery:
        """Write stream to Delta Lake."""
        return (
            df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", f"/tmp/checkpoints/{table_name}")
                .table(table_name)
        )
    
    def write_to_elasticsearch(self, df: DataFrame, index: str) -> StreamingQuery:
        """Write stream to Elasticsearch."""
        return (
            df.writeStream
                .format("org.elasticsearch.spark.sql")
                .outputMode("append")
                .option("checkpointLocation", f"/tmp/checkpoints/es_{index}")
                .option("es.nodes", "elasticsearch")
                .option("es.port", "9200")
                .option("es.resource", index)
                .start()
        )
    
    def run(self):
        """Run streaming job."""
        try:
            logger.info("Starting streaming job")
            
            # Read stream
            events = self.read_stream()
            
            # Process events
            metrics = self.process_events(events)
            
            # Write to Delta Lake
            queries = []
            for name, df in metrics.items():
                # Write to Delta
                delta_query = self.write_to_delta(df, f"metrics_{name}")
                queries.append(delta_query)
                
                # Write to Elasticsearch
                es_query = self.write_to_elasticsearch(df, f"metrics_{name}")
                queries.append(es_query)
            
            # Wait for termination
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Streaming job failed: {e}")
            raise

if __name__ == "__main__":
    # Create Spark session
    spark = (
        SparkSession.builder
            .appName("EventProcessor")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate()
    )
    
    # Run processor
    processor = EventProcessor(spark)
    processor.run() 