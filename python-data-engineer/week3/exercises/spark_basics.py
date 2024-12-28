"""
Spark Basics
- RDD operations
- DataFrame API
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkBasics:
    """Spark basic operations handler."""
    
    def __init__(self, app_name: str = "SparkBasics"):
        """Initialize Spark session."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "1g") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
    
    def rdd_operations(self, data: List[Any]):
        """Demonstrate RDD operations."""
        try:
            # Create RDD
            rdd = self.spark.sparkContext.parallelize(data)
            logger.info(f"Created RDD with {rdd.count()} elements")
            
            # Basic transformations
            mapped_rdd = rdd.map(lambda x: (x, 1))
            filtered_rdd = rdd.filter(lambda x: isinstance(x, (int, float)))
            
            # Actions
            element_count = rdd.count()
            filtered_count = filtered_rdd.count()
            
            # Key-value operations
            if all(isinstance(x, tuple) for x in data):
                reduced_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)
                grouped_rdd = mapped_rdd.groupByKey()
                
                return {
                    "original": rdd.collect(),
                    "mapped": mapped_rdd.collect(),
                    "filtered": filtered_rdd.collect(),
                    "reduced": reduced_rdd.collect(),
                    "grouped": grouped_rdd.mapValues(list).collect()
                }
            
            return {
                "original": rdd.collect(),
                "mapped": mapped_rdd.collect(),
                "filtered": filtered_rdd.collect(),
                "count": element_count,
                "filtered_count": filtered_count
            }
            
        except Exception as e:
            logger.error(f"RDD operation error: {e}")
            raise
    
    def dataframe_operations(
        self,
        data: List[Dict],
        schema: StructType
    ):
        """Demonstrate DataFrame operations."""
        try:
            # Create DataFrame
            df = self.spark.createDataFrame(data, schema)
            logger.info(f"Created DataFrame with {df.count()} rows")
            
            # Show schema
            df.printSchema()
            
            # Basic operations
            numeric_cols = [
                f.name for f in schema.fields 
                if isinstance(
                    f.dataType,
                    (IntegerType, LongType, FloatType, DoubleType)
                )
            ]
            
            # Select and filter
            selected_df = df.select(
                [F.col(c) for c in df.columns[:3]]
            )
            filtered_df = df.filter(
                F.col(numeric_cols[0]) > 0
            ) if numeric_cols else df
            
            # Aggregations
            agg_expressions = [
                F.count("*").alias("row_count"),
                *[
                    F.avg(col).alias(f"avg_{col}")
                    for col in numeric_cols
                ],
                *[
                    F.max(col).alias(f"max_{col}")
                    for col in numeric_cols
                ]
            ]
            
            aggregated = df.agg(*agg_expressions)
            
            # Grouping
            if len(df.columns) > 1:
                grouped = df.groupBy(df.columns[0]).agg(
                    *[
                        F.count(c).alias(f"count_{c}")
                        for c in df.columns[1:]
                    ]
                )
            else:
                grouped = df
            
            # Window functions
            from pyspark.sql.window import Window
            if numeric_cols:
                window_spec = Window.orderBy(numeric_cols[0])
                windowed = df.withColumn(
                    "running_total",
                    F.sum(numeric_cols[0]).over(window_spec)
                )
            else:
                windowed = df
            
            return {
                "original": df,
                "selected": selected_df,
                "filtered": filtered_df,
                "aggregated": aggregated,
                "grouped": grouped,
                "windowed": windowed
            }
            
        except Exception as e:
            logger.error(f"DataFrame operation error: {e}")
            raise
    
    def sql_operations(self, df_dict: Dict[str, Any]):
        """Demonstrate Spark SQL operations."""
        try:
            results = {}
            
            # Register DataFrames as temp views
            for name, df in df_dict.items():
                df.createOrReplaceTempView(name)
                logger.info(f"Registered temp view: {name}")
            
            # Example queries
            queries = {
                "basic_select": f"""
                    SELECT * 
                    FROM {list(df_dict.keys())[0]}
                    LIMIT 5
                """,
                "aggregation": f"""
                    SELECT 
                        COUNT(*) as total_count,
                        {
                            ', '.join([
                                f"AVG({c}) as avg_{c}"
                                for c in df_dict[
                                    list(df_dict.keys())[0]
                                ].columns
                                if c.lower().startswith(('amount', 'price', 'qty'))
                            ])
                        }
                    FROM {list(df_dict.keys())[0]}
                """
            }
            
            # Execute queries
            for name, query in queries.items():
                results[name] = self.spark.sql(query)
            
            return results
            
        except Exception as e:
            logger.error(f"SQL operation error: {e}")
            raise
    
    def cleanup(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function."""
    # Initialize Spark
    spark_basics = SparkBasics("SparkBasicsDemo")
    
    try:
        # RDD example
        rdd_data = [
            ("product1", 100),
            ("product2", 200),
            ("product3", 150)
        ]
        rdd_results = spark_basics.rdd_operations(rdd_data)
        logger.info("RDD Operations Results:")
        for k, v in rdd_results.items():
            logger.info(f"{k}: {v}")
        
        # DataFrame example
        df_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("amount", IntegerType(), True),
            StructField("category", StringType(), True)
        ])
        
        df_data = [
            {"product_id": "P1", "amount": 100, "category": "A"},
            {"product_id": "P2", "amount": 200, "category": "B"},
            {"product_id": "P3", "amount": 150, "category": "A"}
        ]
        
        df_results = spark_basics.dataframe_operations(
            df_data,
            df_schema
        )
        logger.info("DataFrame Operations Results:")
        for k, v in df_results.items():
            logger.info(f"\n{k}:")
            v.show()
        
        # SQL example
        sql_results = spark_basics.sql_operations(
            {"products": df_results["original"]}
        )
        logger.info("SQL Operations Results:")
        for k, v in sql_results.items():
            logger.info(f"\n{k}:")
            v.show()
    
    finally:
        # Cleanup
        spark_basics.cleanup()

if __name__ == "__main__":
    main() 