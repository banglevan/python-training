"""
Test Delta Lake operations.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *
from delta.tables import DeltaTable

from exercises.delta_ops import DeltaOperations

class TestDeltaOperations(unittest.TestCase):
    """Test Delta operations functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Delta operations instance
        cls.delta_ops = DeltaOperations()
        
        # Create temporary directory for Delta tables
        cls.temp_dir = tempfile.mkdtemp()
        
        # Define test schema
        cls.schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("date", DateType(), True)
        ])
        
        # Create test data
        cls.test_data = cls.delta_ops.spark.createDataFrame([
            (1, "A", 10.0, datetime.now().date()),
            (2, "B", 20.0, datetime.now().date())
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.delta_ops.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def get_table_path(self, name: str) -> str:
        """Get path for test table."""
        return os.path.join(self.temp_dir, name)
    
    def test_create_table(self):
        """Test Delta table creation."""
        table_path = self.get_table_path("create_test")
        
        # Create table
        self.delta_ops.create_table(
            table_path,
            self.schema,
            partition_cols=["date"]
        )
        
        # Verify table exists
        delta_table = DeltaTable.forPath(self.delta_ops.spark, table_path)
        self.assertIsNotNone(delta_table)
        
        # Verify schema
        table_schema = delta_table.toDF().schema
        self.assertEqual(table_schema, self.schema)
    
    def test_write_data(self):
        """Test writing data to Delta table."""
        table_path = self.get_table_path("write_test")
        
        # Create table and write data
        self.delta_ops.create_table(table_path, self.schema)
        self.delta_ops.write_data(table_path, self.test_data)
        
        # Verify data
        written_data = self.delta_ops.spark.read.format("delta").load(table_path)
        self.assertEqual(written_data.count(), 2)
        self.assertEqual(
            written_data.select("name").collect(),
            [("A",), ("B",)]
        )
    
    def test_optimize_table(self):
        """Test table optimization."""
        table_path = self.get_table_path("optimize_test")
        
        # Create and write data
        self.delta_ops.create_table(table_path, self.schema)
        self.delta_ops.write_data(table_path, self.test_data)
        
        # Optimize
        self.delta_ops.optimize_table(
            table_path,
            zorder_cols=["id"]
        )
        
        # Verify optimization (check history)
        history = self.delta_ops.get_version_history(table_path)
        self.assertTrue(
            any("OPTIMIZE" in str(h.get('operation'))
                for h in history)
        )
    
    def test_vacuum_table(self):
        """Test table vacuum operation."""
        table_path = self.get_table_path("vacuum_test")
        
        # Create and write data multiple times
        self.delta_ops.create_table(table_path, self.schema)
        for i in range(3):
            self.delta_ops.write_data(table_path, self.test_data)
        
        # Vacuum
        self.delta_ops.vacuum_table(
            table_path,
            retention_hours=0
        )
    
    def test_version_history(self):
        """Test version history retrieval."""
        table_path = self.get_table_path("history_test")
        
        # Create and write data multiple times
        self.delta_ops.create_table(table_path, self.schema)
        self.delta_ops.write_data(table_path, self.test_data)
        self.delta_ops.write_data(table_path, self.test_data)
        
        # Get history
        history = self.delta_ops.get_version_history(table_path)
        self.assertTrue(len(history) >= 2)
        self.assertTrue(all('version' in h for h in history))
    
    def test_restore_version(self):
        """Test version restoration."""
        table_path = self.get_table_path("restore_test")
        
        # Create and write data
        self.delta_ops.create_table(table_path, self.schema)
        self.delta_ops.write_data(table_path, self.test_data)
        
        # Get initial count
        initial_count = self.delta_ops.spark.read.format("delta") \
            .load(table_path).count()
        
        # Write more data
        self.delta_ops.write_data(table_path, self.test_data)
        
        # Restore to version 0
        self.delta_ops.restore_version(table_path, 0)
        
        # Verify restoration
        restored_count = self.delta_ops.spark.read.format("delta") \
            .load(table_path).count()
        self.assertEqual(restored_count, initial_count)
    
    def test_compact_files(self):
        """Test file compaction."""
        table_path = self.get_table_path("compact_test")
        
        # Create and write data multiple times
        self.delta_ops.create_table(table_path, self.schema)
        for _ in range(5):
            self.delta_ops.write_data(table_path, self.test_data)
        
        # Compact files
        self.delta_ops.compact_files(table_path)
        
        # Verify compaction (check history)
        history = self.delta_ops.get_version_history(table_path)
        self.assertTrue(
            any("OPTIMIZE" in str(h.get('operation'))
                for h in history)
        )

if __name__ == '__main__':
    unittest.main() 