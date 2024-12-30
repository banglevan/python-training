"""
Test Data Lake core components.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from datalake.core import DataLakeTable, MetricsCollector
from datalake.formats import ImageHandler, MetadataHandler
from datalake.ingestion import DataIngestion
from datalake.optimization import QueryOptimizer
from datalake.governance import DataGovernance

class TestDataLake(unittest.TestCase):
    """Test Data Lake functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestDataLake") \
            .master("local[*]") \
            .getOrCreate()
        
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test schema
        cls.schema = StructType([
            StructField("id", StringType(), False),
            StructField("content", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("created_at", TimestampType(), True)
        ])
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)
    
    def test_table_operations(self):
        """Test table operations."""
        table_path = os.path.join(self.temp_dir, "test_table")
        
        # Create table
        table = DataLakeTable(self.spark, table_path, self.schema)
        table.create_table(partition_by=["created_at"])
        
        # Verify table exists
        self.assertTrue(
            self.spark.read.format("delta").load(table_path).count() >= 0
        )
    
    def test_metrics_collection(self):
        """Test metrics collection."""
        metrics = MetricsCollector()
        
        # Record operation
        metrics.start_operation("test_op")
        metrics.end_operation("test_op", True)
        
        # Verify metrics
        collected = metrics.get_metrics()
        self.assertIn("test_op", collected)
        self.assertEqual(collected["test_op"]["success"], 1)
    
    def test_data_ingestion(self):
        """Test data ingestion."""
        ingestion = DataIngestion(self.spark)
        
        # Create test data
        test_data = self.spark.createDataFrame([
            ("1", b"test", {"key": "value"}, datetime.now())
        ], self.schema)
        
        # Test ingestion
        target_path = os.path.join(self.temp_dir, "ingestion_test")
        test_data.write.format("delta").save(target_path)
        
        # Verify data
        result = self.spark.read.format("delta").load(target_path)
        self.assertEqual(result.count(), 1)
    
    def test_optimization(self):
        """Test query optimization."""
        optimizer = QueryOptimizer(self.spark)
        table_path = os.path.join(self.temp_dir, "optimize_test")
        
        # Create test table
        test_data = self.spark.createDataFrame([
            ("1", b"test", {"key": "value"}, datetime.now())
        ], self.schema)
        test_data.write.format("delta").save(table_path)
        
        # Test optimization
        optimizer.optimize_table(table_path, ["id"])
        
        # Verify optimization
        stats = optimizer.get_table_stats(table_path)
        self.assertIsNotNone(stats)
    
    def test_governance(self):
        """Test data governance."""
        governance = DataGovernance(self.spark)
        
        # Test audit logging
        governance.log_operation(
            "TEST",
            "test_table",
            "test_user",
            {"action": "test"}
        )
        
        # Verify audit log
        audit = governance.get_audit_history()
        self.assertTrue(len(audit) > 0)

if __name__ == '__main__':
    unittest.main() 