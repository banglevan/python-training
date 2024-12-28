"""Unit tests for Performance Optimization."""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from perf_optimization import SparkOptimizer, PerformanceMetrics

class TestSparkOptimizer(unittest.TestCase):
    """Test cases for Spark Optimizer."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Initialize optimizer
        cls.optimizer = SparkOptimizer("TestOptimizer")
        
        # Create sample data
        data = [(i, f"value_{i % 100}") for i in range(10000)]
        cls.sample_df = cls.optimizer.spark.createDataFrame(
            data,
            ["id", "value"]
        )
        
        # Create large sample data
        cls.large_df = cls.optimizer.spark.createDataFrame(
            [(i, f"value_{i % 1000}") for i in range(100000)],
            ["id", "value"]
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.optimizer:
            cls.optimizer.cleanup()
    
    def test_optimize_partitioning(self):
        """Test partitioning optimization."""
        # Test with default parameters
        result_df, metrics = self.optimizer.optimize_partitioning(
            self.sample_df
        )
        
        self.assertIsNotNone(result_df)
        self.assertIsInstance(metrics, PerformanceMetrics)
        self.assertTrue(10 <= metrics.num_partitions <= 200)
        self.assertTrue(metrics.execution_time > 0)
        
        # Test with specific partition columns
        result_df, metrics = self.optimizer.optimize_partitioning(
            self.sample_df,
            partition_columns=["value"],
            target_size="64MB"
        )
        
        self.assertIsNotNone(result_df)
        self.assertTrue(
            metrics.records_per_partition > 0
        )
    
    def test_optimize_memory(self):
        """Test memory optimization."""
        result_df, metrics = self.optimizer.optimize_memory(
            self.sample_df
        )
        
        self.assertIsNotNone(result_df)
        self.assertIsInstance(metrics, PerformanceMetrics)
        self.assertIn('max', metrics.memory_usage)
        
        # Test with string columns
        string_df = self.optimizer.spark.createDataFrame(
            [("A",), ("B",), ("A",), ("C",), ("B",)],
            ["category"]
        )
        result_df, metrics = self.optimizer.optimize_memory(
            string_df
        )
        
        self.assertIsNotNone(result_df)
    
    def test_tune_job(self):
        """Test job tuning."""
        operation = {
            "type": "aggregation",
            "params": {
                "group_by": ["value"],
                "aggregations": [
                    F.count("id").alias("count"),
                    F.sum("id").alias("sum")
                ],
                "shuffle_partitions": 20
            }
        }
        
        result_df, metrics = self.optimizer.tune_job(
            self.sample_df,
            operation
        )
        
        self.assertIsNotNone(result_df)
        self.assertIsInstance(metrics, PerformanceMetrics)
        self.assertTrue(metrics.execution_time > 0)
        
        # Verify result
        result_count = result_df.count()
        self.assertTrue(result_count > 0)
    
    def test_parse_size(self):
        """Test size string parsing."""
        self.assertEqual(
            self.optimizer._parse_size("1KB"),
            1024
        )
        self.assertEqual(
            self.optimizer._parse_size("1MB"),
            1024 * 1024
        )
        self.assertEqual(
            self.optimizer._parse_size("1GB"),
            1024 * 1024 * 1024
        )
    
    def test_should_cache(self):
        """Test cache decision logic."""
        # Create a DataFrame with multiple scans
        df1 = self.sample_df
        df2 = df1.union(df1)
        
        self.assertTrue(
            self.optimizer._should_cache(df2)
        )
    
    def test_determine_storage_level(self):
        """Test storage level determination."""
        from pyspark import StorageLevel
        
        # Test with small DataFrame
        level = self.optimizer._determine_storage_level(
            self.sample_df
        )
        self.assertEqual(
            level,
            StorageLevel.MEMORY_AND_DISK
        )
        
        # Test with large DataFrame
        level = self.optimizer._determine_storage_level(
            self.large_df
        )
        self.assertEqual(
            level,
            StorageLevel.MEMORY_AND_DISK_SER
        )
    
    def test_get_memory_metrics(self):
        """Test memory metrics collection."""
        metrics = self.optimizer._get_memory_metrics()
        
        self.assertIsInstance(metrics, dict)
        self.assertIn('max', metrics)
        self.assertTrue(metrics['max'] > 0)
    
    def test_get_stage_metrics(self):
        """Test stage metrics collection."""
        metrics = self.optimizer._get_stage_metrics()
        
        self.assertIsInstance(metrics, dict)
        self.assertIn('completed_stages', metrics)
        self.assertIn('active_stages', metrics)
    
    def test_execute_operation(self):
        """Test operation execution."""
        # Test join operation
        other_df = self.optimizer.spark.createDataFrame(
            [(i, f"other_{i}") for i in range(100)],
            ["id", "other_value"]
        )
        
        operation = {
            "type": "join",
            "params": {
                "other_df": other_df,
                "on": "id",
                "how": "inner"
            }
        }
        
        result = self.optimizer._execute_operation(
            self.sample_df,
            operation
        )
        
        self.assertIsNotNone(result)
        self.assertTrue("other_value" in result.columns)
        
        # Test aggregation operation
        operation = {
            "type": "aggregation",
            "params": {
                "group_by": ["value"],
                "aggregations": [
                    F.count("id").alias("count")
                ]
            }
        }
        
        result = self.optimizer._execute_operation(
            self.sample_df,
            operation
        )
        
        self.assertIsNotNone(result)
        self.assertTrue("count" in result.columns)

if __name__ == '__main__':
    unittest.main() 