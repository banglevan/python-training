"""
Test Delta Lake features.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *
from delta.tables import DeltaTable

from exercises.delta_features import DeltaFeatures

class TestDeltaFeatures(unittest.TestCase):
    """Test Delta features functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Delta features instance
        cls.delta_features = DeltaFeatures()
        
        # Create temporary directory
        cls.temp_dir = tempfile.mkdtemp()
        
        # Define schema
        cls.schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        # Create test data
        cls.test_data = cls.delta_features.spark.createDataFrame([
            (1, "A", 10.0, datetime.now()),
            (2, "B", 20.0, datetime.now())
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.delta_features.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def get_table_path(self, name: str) -> str:
        """Get path for test table."""
        return os.path.join(self.temp_dir, name)
    
    def test_enforce_schema(self):
        """Test schema enforcement."""
        table_path = self.get_table_path("schema_test")
        
        # Create table
        self.test_data.write.format("delta").save(table_path)
        
        # Enforce schema
        self.delta_features.enforce_schema(
            table_path,
            self.schema,
            mode="append"
        )
        
        # Try to write data with wrong schema
        wrong_schema = StructType([
            StructField("wrong_id", IntegerType(), False)
        ])
        wrong_data = self.delta_features.spark.createDataFrame(
            [(1,)],
            wrong_schema
        )
        
        # Should raise exception
        with self.assertRaises(Exception):
            wrong_data.write.format("delta").mode("append").save(table_path)
    
    def test_merge_data(self):
        """Test MERGE operation."""
        table_path = self.get_table_path("merge_test")
        
        # Create initial data
        self.test_data.write.format("delta").save(table_path)
        
        # Create update data
        update_data = self.delta_features.spark.createDataFrame([
            (1, "A_updated", 15.0, datetime.now()),
            (3, "C", 30.0, datetime.now())
        ], self.schema)
        
        # Perform merge
        self.delta_features.merge_data(
            table_path,
            update_data,
            "target.id = source.id",
            "UPDATE SET *",
            "INSERT *"
        )
        
        # Verify results
        result = self.delta_features.spark.read.format("delta").load(table_path)
        self.assertEqual(result.count(), 3)
        updated = result.filter("id = 1").collect()[0]
        self.assertEqual(updated.name, "A_updated")
    
    def test_streaming_setup(self):
        """Test streaming setup."""
        table_path = self.get_table_path("stream_test")
        checkpoint_path = self.get_table_path("checkpoints")
        
        # Create table
        self.test_data.write.format("delta").save(table_path)
        
        # Setup streaming
        self.delta_features.setup_streaming(
            table_path,
            checkpoint_path
        )
        
        # Verify CDF is enabled
        table = DeltaTable.forPath(self.delta_features.spark, table_path)
        props = table.detail().select("properties").collect()[0][0]
        self.assertEqual(props.get("delta.enableChangeDataFeed"), "true")
    
    def test_write_stream(self):
        """Test stream writing."""
        table_path = self.get_table_path("write_stream_test")
        checkpoint_path = self.get_table_path("write_checkpoints")
        
        # Create streaming source
        streaming_source = self.delta_features.spark \
            .readStream \
            .format("rate") \
            .option("rowsPerSecond", 1) \
            .load()
        
        # Start streaming write
        query = self.delta_features.write_stream(
            table_path,
            streaming_source,
            checkpoint_path,
            "1 minute"
        )
        
        # Wait briefly
        query.awaitTermination(10)
        query.stop()
        
        # Verify data was written
        result = self.delta_features.spark.read.format("delta").load(table_path)
        self.assertTrue(result.count() > 0)
    
    def test_read_stream(self):
        """Test stream reading."""
        table_path = self.get_table_path("read_stream_test")
        
        # Create table with CDF enabled
        self.test_data.write.format("delta").save(table_path)
        self.delta_features.setup_streaming(
            table_path,
            self.get_table_path("read_checkpoints")
        )
        
        # Start stream reader
        stream_df = self.delta_features.read_stream(table_path)
        self.assertTrue(stream_df.isStreaming)

if __name__ == '__main__':
    unittest.main() 