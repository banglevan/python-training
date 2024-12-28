"""
Data Processing
- Transformations
- Actions
- Caching
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from typing import List, Dict, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Spark data processing handler."""
    
    def __init__(self, app_name: str = "DataProcessor"):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
    
    def load_data(
        self,
        path: str,
        format: str = "csv",
        options: Dict = None
    ):
        """Load data from source."""
        try:
            reader = self.spark.read.format(format)
            
            if options:
                reader = reader.options(**options)
            
            df = reader.load(path)
            logger.info(
                f"Loaded {df.count()} rows from {path}"
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Data loading error: {e}")
            raise
    
    def apply_transformations(
        self,
        df,
        transformations: List[Dict]
    ):
        """Apply series of transformations."""
        try:
            result = df
            
            for transform in transformations:
                op_type = transform.get("type")
                params = transform.get("params", {})
                
                if op_type == "select":
                    result = result.select(*params["columns"])
                
                elif op_type == "filter":
                    result = result.filter(params["condition"])
                
                elif op_type == "groupBy":
                    result = result.groupBy(
                        *params["columns"]
                    ).agg(*params["aggregations"])
                
                elif op_type == "window":
                    window_spec = Window.partitionBy(
                        *params.get("partition_by", [])
                    ).orderBy(*params.get("order_by", []))
                    
                    for col, func in params.get(
                        "window_functions",
                        {}
                    ).items():
                        result = result.withColumn(
                            col,
                            func.over(window_spec)
                        )
                
                elif op_type == "join":
                    result = result.join(
                        params["other_df"],
                        on=params["on"],
                        how=params.get("how", "inner")
                    )
                
                elif op_type == "withColumn":
                    result = result.withColumn(
                        params["name"],
                        params["expression"]
                    )
                
                # Cache if specified
                if transform.get("cache", False):
                    result.cache()
                    logger.info(
                        f"Cached DataFrame after {op_type}"
                    )
            
            return result
            
        except Exception as e:
            logger.error(f"Transformation error: {e}")
            raise
    
    def optimize_execution(self, df):
        """Optimize DataFrame execution."""
        try:
            # Repartition if needed
            num_partitions = df.rdd.getNumPartitions()
            
            if num_partitions < 10:
                df = df.repartition(10)
                logger.info(
                    "Increased partitions to 10"
                )
            elif num_partitions > 200:
                df = df.coalesce(200)
                logger.info(
                    "Reduced partitions to 200"
                )
            
            # Cache if beneficial
            if self._should_cache(df):
                df.cache()
                logger.info("DataFrame cached")
            
            return df
            
        except Exception as e:
            logger.error(f"Optimization error: {e}")
            raise
    
    def _should_cache(self, df):
        """Determine if DataFrame should be cached."""
        # Check if DataFrame will be used multiple times
        logical_plan = df._jdf.queryExecution().analyzed()
        num_actions = str(logical_plan).count("Action")
        
        return num_actions > 1
    
    def execute_actions(
        self,
        df,
        actions: List[Dict]
    ):
        """Execute series of actions."""
        try:
            results = {}
            
            for action in actions:
                action_type = action.get("type")
                params = action.get("params", {})
                
                if action_type == "count":
                    results["count"] = df.count()
                
                elif action_type == "collect":
                    results["collected"] = df.collect()
                
                elif action_type == "show":
                    df.show(
                        n=params.get("n", 20),
                        truncate=params.get("truncate", True)
                    )
                
                elif action_type == "write":
                    writer = df.write \
                        .format(params.get("format", "parquet"))
                    
                    if "mode" in params:
                        writer = writer.mode(params["mode"])
                    
                    if "options" in params:
                        writer = writer.options(
                            **params["options"]
                        )
                    
                    writer.save(params["path"])
                    logger.info(
                        f"Data written to {params['path']}"
                    )
            
            return results
            
        except Exception as e:
            logger.error(f"Action execution error: {e}")
            raise
    
    def cleanup(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function."""
    processor = DataProcessor("DataProcessingDemo")
    
    try:
        # Load sample data
        df = processor.load_data(
            "data/sales.csv",
            options={
                "header": "true",
                "inferSchema": "true"
            }
        )
        
        # Define transformations
        transformations = [
            {
                "type": "select",
                "params": {
                    "columns": [
                        "date",
                        "product_id",
                        "amount"
                    ]
                }
            },
            {
                "type": "withColumn",
                "params": {
                    "name": "year_month",
                    "expression": F.date_format(
                        "date",
                        "yyyy-MM"
                    )
                }
            },
            {
                "type": "groupBy",
                "params": {
                    "columns": ["year_month"],
                    "aggregations": [
                        F.sum("amount").alias(
                            "total_amount"
                        ),
                        F.count("*").alias(
                            "num_transactions"
                        )
                    ]
                },
                "cache": True
            }
        ]
        
        # Apply transformations
        result_df = processor.apply_transformations(
            df,
            transformations
        )
        
        # Optimize
        result_df = processor.optimize_execution(
            result_df
        )
        
        # Define actions
        actions = [
            {"type": "show", "params": {"n": 10}},
            {
                "type": "write",
                "params": {
                    "path": "output/sales_summary",
                    "format": "parquet",
                    "mode": "overwrite"
                }
            }
        ]
        
        # Execute actions
        processor.execute_actions(result_df, actions)
        
    finally:
        processor.cleanup()

if __name__ == "__main__":
    main() 