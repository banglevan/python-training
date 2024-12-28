"""
Performance Optimization
- Partitioning
- Memory management
- Job tuning
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
from typing import Dict, List, Any
import time
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class PerformanceMetrics:
    """Performance metrics container."""
    execution_time: float
    num_partitions: int
    records_per_partition: float
    memory_usage: Dict[str, float]
    stage_metrics: Dict[str, Any]

class SparkOptimizer:
    """Spark performance optimization handler."""
    
    def __init__(self, app_name: str = "SparkOptimizer"):
        """Initialize Spark session with tuning."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.speculation", "true") \
            .getOrCreate()
        
        logger.info("Spark session initialized with optimizations")
    
    def optimize_partitioning(
        self,
        df,
        partition_columns: List[str] = None,
        target_size: str = "128MB"
    ):
        """Optimize DataFrame partitioning."""
        try:
            start_time = time.time()
            original_partitions = df.rdd.getNumPartitions()
            
            # Calculate optimal number of partitions
            total_size = df.count() * \
                sum(df.schema[c].dataType.defaultSize \
                    for c in df.columns)
            target_bytes = self._parse_size(target_size)
            optimal_partitions = max(
                10,
                min(
                    200,
                    int(total_size / target_bytes)
                )
            )
            
            # Repartition DataFrame
            if partition_columns:
                result = df.repartition(
                    optimal_partitions,
                    *partition_columns
                )
            else:
                result = df.coalesce(optimal_partitions) \
                    if optimal_partitions < original_partitions \
                    else df.repartition(optimal_partitions)
            
            # Force computation to measure metrics
            result.cache()
            count = result.count()
            
            metrics = PerformanceMetrics(
                execution_time=time.time() - start_time,
                num_partitions=result.rdd.getNumPartitions(),
                records_per_partition=count / optimal_partitions,
                memory_usage=self._get_memory_metrics(),
                stage_metrics=self._get_stage_metrics()
            )
            
            logger.info(
                f"Partitioning optimized: "
                f"{original_partitions} -> {optimal_partitions} partitions"
            )
            
            return result, metrics
            
        except Exception as e:
            logger.error(f"Partitioning optimization error: {e}")
            raise
    
    def optimize_memory(self, df):
        """Optimize memory usage."""
        try:
            start_time = time.time()
            
            # Analyze column cardinality
            column_stats = {}
            for col in df.columns:
                distinct_count = df.select(col).distinct().count()
                column_stats[col] = {
                    "distinct_count": distinct_count,
                    "type": str(df.schema[col].dataType)
                }
            
            # Optimize string columns with low cardinality
            for col, stats in column_stats.items():
                if "string" in stats["type"].lower() and \
                   stats["distinct_count"] < 1000:
                    df = df.withColumn(
                        col,
                        F.broadcast(
                            df.select(col).distinct()
                        ).alias(col)
                    )
            
            # Cache if beneficial
            if self._should_cache(df):
                storage_level = self._determine_storage_level(df)
                df = df.persist(storage_level)
                logger.info(
                    f"DataFrame cached with {storage_level}"
                )
            
            metrics = PerformanceMetrics(
                execution_time=time.time() - start_time,
                num_partitions=df.rdd.getNumPartitions(),
                records_per_partition=df.count() / \
                    df.rdd.getNumPartitions(),
                memory_usage=self._get_memory_metrics(),
                stage_metrics=self._get_stage_metrics()
            )
            
            return df, metrics
            
        except Exception as e:
            logger.error(f"Memory optimization error: {e}")
            raise
    
    def tune_job(self, df, operation: Dict):
        """Tune specific Spark job."""
        try:
            start_time = time.time()
            
            # Set operation-specific configurations
            if operation["type"] == "join":
                self.spark.conf.set(
                    "spark.sql.autoBroadcastJoinThreshold",
                    operation.get("broadcast_threshold", "10m")
                )
                self.spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    operation.get("shuffle_partitions", "200")
                )
            
            elif operation["type"] == "aggregation":
                self.spark.conf.set(
                    "spark.sql.shuffle.partitions",
                    operation.get("shuffle_partitions", "200")
                )
            
            # Execute operation
            result = self._execute_operation(df, operation)
            
            metrics = PerformanceMetrics(
                execution_time=time.time() - start_time,
                num_partitions=result.rdd.getNumPartitions(),
                records_per_partition=result.count() / \
                    result.rdd.getNumPartitions(),
                memory_usage=self._get_memory_metrics(),
                stage_metrics=self._get_stage_metrics()
            )
            
            return result, metrics
            
        except Exception as e:
            logger.error(f"Job tuning error: {e}")
            raise
    
    def _parse_size(self, size_str: str) -> int:
        """Parse size string to bytes."""
        units = {
            'B': 1,
            'KB': 1024,
            'MB': 1024**2,
            'GB': 1024**3
        }
        number = float(size_str[:-2])
        unit = size_str[-2:].upper()
        return int(number * units[unit])
    
    def _should_cache(self, df) -> bool:
        """Determine if DataFrame should be cached."""
        plan = df._jdf.queryExecution().analyzed()
        return str(plan).count("Scan") > 1
    
    def _determine_storage_level(self, df):
        """Determine optimal storage level."""
        from pyspark import StorageLevel
        
        # Estimate memory usage
        sample_size = min(df.count(), 1000)
        memory_per_row = sum(
            df.schema[c].dataType.defaultSize 
            for c in df.columns
        )
        total_memory = memory_per_row * df.count()
        
        if total_memory < 0.5 * self._get_memory_metrics()['max']:
            return StorageLevel.MEMORY_AND_DISK
        else:
            return StorageLevel.MEMORY_AND_DISK_SER
    
    def _get_memory_metrics(self) -> Dict[str, float]:
        """Get memory usage metrics."""
        metrics = {}
        
        # Get metrics from Spark context
        sc_metrics = self.spark.sparkContext.getConf().getAll()
        
        metrics['max'] = float(
            dict(sc_metrics).get(
                'spark.driver.memory',
                '1g'
            )[:-1]
        ) * 1024
        
        return metrics
    
    def _get_stage_metrics(self) -> Dict[str, Any]:
        """Get execution stage metrics."""
        # This is a simplified version
        return {
            'completed_stages': len(
                self.spark.sparkContext.statusTracker(
                ).getStageInfo()
            ),
            'active_stages': len(
                self.spark.sparkContext.statusTracker(
                ).getActiveStageIds()
            )
        }
    
    def _execute_operation(self, df, operation: Dict):
        """Execute specific operation."""
        op_type = operation["type"]
        params = operation.get("params", {})
        
        if op_type == "join":
            return df.join(
                params["other_df"],
                on=params["on"],
                how=params.get("how", "inner")
            )
        
        elif op_type == "aggregation":
            return df.groupBy(
                *params.get("group_by", [])
            ).agg(*params.get("aggregations", []))
        
        return df
    
    def cleanup(self):
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function."""
    optimizer = SparkOptimizer("OptimizationDemo")
    
    try:
        # Create sample DataFrame
        data = [(i, f"value_{i}") for i in range(1000000)]
        df = optimizer.spark.createDataFrame(
            data,
            ["id", "value"]
        )
        
        # 1. Optimize partitioning
        logger.info("Optimizing partitioning...")
        partitioned_df, part_metrics = optimizer.optimize_partitioning(
            df,
            partition_columns=["id"],
            target_size="64MB"
        )
        logger.info(f"Partitioning metrics: {part_metrics}")
        
        # 2. Optimize memory
        logger.info("Optimizing memory...")
        memory_df, mem_metrics = optimizer.optimize_memory(
            partitioned_df
        )
        logger.info(f"Memory metrics: {mem_metrics}")
        
        # 3. Tune specific job
        logger.info("Tuning aggregation job...")
        operation = {
            "type": "aggregation",
            "params": {
                "group_by": ["value"],
                "aggregations": [
                    F.count("id").alias("count"),
                    F.sum("id").alias("sum")
                ],
                "shuffle_partitions": 50
            }
        }
        
        result_df, job_metrics = optimizer.tune_job(
            memory_df,
            operation
        )
        logger.info(f"Job metrics: {job_metrics}")
        
        # Show results
        result_df.show(5)
        
    finally:
        optimizer.cleanup()

if __name__ == "__main__":
    main() 