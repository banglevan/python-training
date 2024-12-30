"""
Batch processor for historical data analysis.
"""

from typing import Dict, Any, List
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, window, count, sum, avg
from src.utils.config import Config
from src.utils.logging import get_logger
from src.utils.metrics import MetricsTracker

logger = get_logger(__name__)

class HistoricalProcessor:
    """Processor for historical data analysis."""
    
    def __init__(self, spark: SparkSession, config: Config):
        """Initialize processor."""
        self.spark = spark
        self.config = config
        self.metrics = MetricsTracker()
    
    def process_daily_metrics(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, DataFrame]:
        """
        Process daily metrics for date range.
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            Dictionary of metric DataFrames
        """
        try:
            self.metrics.start_operation('daily_metrics')
            
            # Read events from Delta Lake
            events_df = self.spark.read.format("delta").load(
                f"{self.config.storage.delta_path}/events"
            ).filter(
                (col("timestamp") >= start_date) &
                (col("timestamp") < end_date)
            )
            
            # Calculate daily sales metrics
            sales_metrics = self._calculate_sales_metrics(events_df)
            
            # Calculate daily user metrics
            user_metrics = self._calculate_user_metrics(events_df)
            
            # Calculate daily product metrics
            product_metrics = self._calculate_product_metrics(events_df)
            
            duration = self.metrics.end_operation('daily_metrics')
            logger.info(f"Processed daily metrics in {duration:.3f} seconds")
            
            return {
                'sales': sales_metrics,
                'users': user_metrics,
                'products': product_metrics
            }
            
        except Exception as e:
            logger.error(f"Failed to process daily metrics: {e}")
            raise
    
    def _calculate_sales_metrics(self, events_df: DataFrame) -> DataFrame:
        """Calculate sales metrics."""
        return (
            events_df
            .filter(col("event_type") == "purchase")
            .groupBy(
                window("timestamp", "1 day"),
                "product_id"
            )
            .agg(
                sum("quantity").alias("total_quantity"),
                sum(col("price") * col("quantity")).alias("total_revenue"),
                count("event_id").alias("num_purchases")
            )
        )
    
    def _calculate_user_metrics(self, events_df: DataFrame) -> DataFrame:
        """Calculate user metrics."""
        return (
            events_df
            .groupBy(
                window("timestamp", "1 day"),
                "user_id"
            )
            .agg(
                count("session_id").alias("num_sessions"),
                count(
                    when(col("event_type") == "view", True)
                ).alias("num_views"),
                count(
                    when(col("event_type") == "purchase", True)
                ).alias("num_purchases")
            )
        )
    
    def _calculate_product_metrics(self, events_df: DataFrame) -> DataFrame:
        """Calculate product metrics."""
        return (
            events_df
            .groupBy(
                window("timestamp", "1 day"),
                "product_id"
            )
            .agg(
                count(
                    when(col("event_type") == "view", True)
                ).alias("views"),
                count(
                    when(col("event_type") == "cart", True)
                ).alias("cart_adds"),
                avg("price").alias("avg_price")
            )
        )
    
    def save_metrics(
        self,
        metrics: Dict[str, DataFrame],
        date: datetime
    ) -> None:
        """
        Save metrics to storage.
        
        Args:
            metrics: Dictionary of metric DataFrames
            date: Processing date
        """
        try:
            self.metrics.start_operation('save_metrics')
            
            for name, df in metrics.items():
                # Save to Delta Lake
                table_path = f"{self.config.storage.delta_path}/metrics_{name}"
                (
                    df.write
                    .format("delta")
                    .mode("append")
                    .save(table_path)
                )
                
                # Save to Elasticsearch
                (
                    df.write
                    .format("org.elasticsearch.spark.sql")
                    .option("es.nodes", self.config.storage.elasticsearch_host)
                    .option("es.port", self.config.storage.elasticsearch_port)
                    .option("es.resource", f"metrics_{name}")
                    .mode("append")
                    .save()
                )
            
            duration = self.metrics.end_operation('save_metrics')
            logger.info(f"Saved metrics in {duration:.3f} seconds")
            
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")
            raise 