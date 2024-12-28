"""
Customer Analytics
- Process large datasets
- Customer segmentation
- Behavior analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerAnalytics:
    """Customer analytics processor."""
    
    def __init__(self, app_name: str = "CustomerAnalytics"):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
    
    def load_customer_data(
        self,
        transactions_path: str,
        customers_path: str
    ):
        """Load and prepare customer data."""
        try:
            # Load transactions
            transactions_df = self.spark.read.parquet(
                transactions_path
            )
            logger.info(
                f"Loaded {transactions_df.count()} transactions"
            )
            
            # Load customer info
            customers_df = self.spark.read.parquet(
                customers_path
            )
            logger.info(
                f"Loaded {customers_df.count()} customers"
            )
            
            # Join data
            df = customers_df.join(
                transactions_df,
                "customer_id",
                "left"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Data loading error: {e}")
            raise
    
    def calculate_customer_metrics(self, df):
        """Calculate customer-level metrics."""
        try:
            # Define time windows
            current_date = df.select(
                F.max("transaction_date")
            ).collect()[0][0]
            
            # Calculate RFM metrics
            window_spec = Window.partitionBy("customer_id")
            
            metrics_df = df.withColumn(
                "days_since_last_purchase",
                F.datediff(
                    F.lit(current_date),
                    F.max("transaction_date").over(window_spec)
                )
            ).withColumn(
                "purchase_frequency",
                F.count("transaction_id").over(window_spec)
            ).withColumn(
                "total_amount",
                F.sum("amount").over(window_spec)
            ).withColumn(
                "avg_basket_size",
                F.avg("amount").over(window_spec)
            ).withColumn(
                "distinct_products",
                F.countDistinct("product_id").over(window_spec)
            )
            
            # Aggregate to customer level
            customer_metrics = metrics_df.select(
                "customer_id",
                "days_since_last_purchase",
                "purchase_frequency",
                "total_amount",
                "avg_basket_size",
                "distinct_products"
            ).distinct()
            
            return customer_metrics
            
        except Exception as e:
            logger.error(f"Metrics calculation error: {e}")
            raise
    
    def segment_customers(
        self,
        metrics_df,
        num_clusters: int = 5
    ):
        """Perform customer segmentation."""
        try:
            # Prepare features
            feature_cols = [
                "days_since_last_purchase",
                "purchase_frequency",
                "total_amount",
                "avg_basket_size",
                "distinct_products"
            ]
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features"
            )
            
            # Scale features
            from pyspark.ml.feature import StandardScaler
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features"
            )
            
            # Train KMeans
            kmeans = KMeans(
                k=num_clusters,
                featuresCol="scaled_features",
                predictionCol="segment"
            )
            
            # Build pipeline
            from pyspark.ml import Pipeline
            pipeline = Pipeline(stages=[
                assembler,
                scaler,
                kmeans
            ])
            
            # Fit model
            model = pipeline.fit(metrics_df)
            segmented_df = model.transform(metrics_df)
            
            # Evaluate clustering
            evaluator = ClusteringEvaluator(
                featuresCol="scaled_features"
            )
            silhouette = evaluator.evaluate(segmented_df)
            
            logger.info(f"Silhouette score: {silhouette}")
            
            return segmented_df, model
            
        except Exception as e:
            logger.error(f"Segmentation error: {e}")
            raise
    
    def analyze_behavior(self, df, segmented_df):
        """Analyze customer behavior by segment."""
        try:
            # Join segment info
            behavior_df = df.join(
                segmented_df.select(
                    "customer_id",
                    "segment"
                ),
                "customer_id"
            )
            
            # Analyze patterns
            patterns = {}
            
            # 1. Product preferences by segment
            patterns["product_preferences"] = behavior_df.groupBy(
                "segment",
                "product_category"
            ).agg(
                F.count("*").alias("purchase_count"),
                F.sum("amount").alias("total_amount")
            ).orderBy("segment", F.desc("purchase_count"))
            
            # 2. Time patterns
            patterns["time_patterns"] = behavior_df.withColumn(
                "hour_of_day",
                F.hour("transaction_time")
            ).withColumn(
                "day_of_week",
                F.dayofweek("transaction_date")
            ).groupBy("segment", "hour_of_day", "day_of_week").agg(
                F.count("*").alias("transaction_count")
            )
            
            # 3. Basket analysis
            patterns["basket_analysis"] = behavior_df.groupBy(
                "segment",
                "transaction_id"
            ).agg(
                F.collect_list("product_id").alias("basket"),
                F.sum("amount").alias("basket_value")
            )
            
            return patterns
            
        except Exception as e:
            logger.error(f"Behavior analysis error: {e}")
            raise
    
    def generate_insights(
        self,
        segmented_df,
        behavior_patterns: Dict
    ):
        """Generate business insights."""
        try:
            insights = {
                "segment_profiles": {},
                "recommendations": []
            }
            
            # 1. Segment profiles
            for segment in range(5):  # Assuming 5 segments
                segment_data = segmented_df.filter(
                    F.col("segment") == segment
                )
                
                profile = {
                    "size": segment_data.count(),
                    "avg_value": segment_data.select(
                        F.avg("total_amount")
                    ).collect()[0][0],
                    "frequency": segment_data.select(
                        F.avg("purchase_frequency")
                    ).collect()[0][0],
                    "recency": segment_data.select(
                        F.avg("days_since_last_purchase")
                    ).collect()[0][0]
                }
                
                insights["segment_profiles"][
                    f"Segment_{segment}"
                ] = profile
            
            # 2. Key findings
            for segment, profile in insights[
                "segment_profiles"
            ].items():
                if profile["avg_value"] > 1000 and \
                   profile["frequency"] > 10:
                    insights["recommendations"].append(
                        f"High-value loyalty program for {segment}"
                    )
                elif profile["recency"] > 60:
                    insights["recommendations"].append(
                        f"Re-engagement campaign for {segment}"
                    )
            
            return insights
            
        except Exception as e:
            logger.error(f"Insights generation error: {e}")
            raise
    
    def save_results(
        self,
        segmented_df,
        patterns: Dict,
        insights: Dict,
        output_path: str
    ):
        """Save analysis results."""
        try:
            # Save segmented customers
            segmented_df.write.mode("overwrite").parquet(
                f"{output_path}/segmented_customers"
            )
            
            # Save patterns
            for name, df in patterns.items():
                df.write.mode("overwrite").parquet(
                    f"{output_path}/patterns/{name}"
                )
            
            # Save insights
            insights_df = self.spark.createDataFrame([
                (k, str(v))
                for k, v in insights.items()
            ], ["key", "value"])
            
            insights_df.write.mode("overwrite").parquet(
                f"{output_path}/insights"
            )
            
            logger.info(f"Results saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Results saving error: {e}")
            raise
    
    def cleanup(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function."""
    analytics = CustomerAnalytics("CustomerAnalyticsDemo")
    
    try:
        # Load data
        df = analytics.load_customer_data(
            "data/transactions.parquet",
            "data/customers.parquet"
        )
        
        # Calculate metrics
        metrics_df = analytics.calculate_customer_metrics(df)
        
        # Segment customers
        segmented_df, model = analytics.segment_customers(
            metrics_df
        )
        
        # Analyze behavior
        patterns = analytics.analyze_behavior(
            df,
            segmented_df
        )
        
        # Generate insights
        insights = analytics.generate_insights(
            segmented_df,
            patterns
        )
        
        # Save results
        analytics.save_results(
            segmented_df,
            patterns,
            insights,
            "output/customer_analytics"
        )
        
        # Print summary
        logger.info("\nCustomer Segments Summary:")
        segmented_df.groupBy("segment").agg(
            F.count("*").alias("count"),
            F.avg("total_amount").alias("avg_amount"),
            F.avg("purchase_frequency").alias("avg_frequency")
        ).orderBy("segment").show()
        
    finally:
        analytics.cleanup()

if __name__ == "__main__":
    main() 