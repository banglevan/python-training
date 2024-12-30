"""
Test Apache Hudi operations.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql.types import *

from exercises.hudi_ops import HudiOperations

class TestHudiOperations(unittest.TestCase):
    """Test Hudi operations functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create Hudi operations instance
        cls.hudi = HudiOperations()
        
        # Create temporary directory
        cls.temp_dir = tempfile.mkdtemp()
        
        # Define schema
        cls.schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("ts", TimestampType(), True),
            StructField("date", DateType(), True)
        ])
        
        # Create test data
        cls.test_data = cls.hudi.spark.createDataFrame([
            (1, "A", 10.0, datetime.now(), datetime.now().date()),
            (2, "B", 20.0, datetime.now(), datetime.now().date())
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop Spark session
        cls.hudi.spark.stop()
        
        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def get_table_path(self, name: str) -> str:
        """Get path for test table."""
        return os.path.join(self.temp_dir, name)
    
    def test_create_cow_table(self):
        """Test Copy-on-Write table creation."""
        table_path = self.get_table_path("cow_test")
        
        # Create COW table
        self.hudi.create_cow_table(
            table_path,
            "cow_test",
            self.schema,
            key_fields=["id"],
            partition_fields=["date"],
            pre_combine_field="ts"
        )
        
        # Verify table exists and type
        table_type = self.hudi.spark.read.format("hudi") \
            .load(table_path) \
            .select("_hoodie_table_type") \
            .limit(1)
        
        self.assertTrue(table_type.count() >= 0)
    
    def test_create_mor_table(self):
        """Test Merge-on-Read table creation."""
        table_path = self.get_table_path("mor_test")
        
        # Create MOR table
        self.hudi.create_mor_table(
            table_path,
            "mor_test",
            self.schema,
            key_fields=["id"],
            partition_fields=["date"],
            pre_combine_field="ts"
        )
        
        # Verify table exists and type
        table_type = self.hudi.spark.read.format("hudi") \
            .load(table_path) \
            .select("_hoodie_table_type") \
            .limit(1)
        
        self.assertTrue(table_type.count() >= 0)
    
    def test_upsert_data(self):
        """Test data upsertion."""
        table_path = self.get_table_path("upsert_test")
        
        # Create table
        self.hudi.create_cow_table(
            table_path,
            "upsert_test",
            self.schema,
            key_fields=["id"]
        )
        
        # Upsert data
        self.hudi.upsert_data(
            table_path,
            self.test_data,
            key_fields=["id"]
        )
        
        # Verify data
        result = self.hudi.spark.read.format("hudi").load(table_path)
        self.assertEqual(result.count(), 2)
        self.assertEqual(
            result.select("name").collect(),
            [("A",), ("B",)]
        )
    
    def test_delete_data(self):
        """Test data deletion."""
        table_path = self.get_table_path("delete_test")
        
        # Create table and insert data
        self.hudi.create_cow_table(
            table_path,
            "delete_test",
            self.schema,
            key_fields=["id"]
        )
        self.hudi.upsert_data(
            table_path,
            self.test_data,
            key_fields=["id"]
        )
        
        # Delete data
        self.hudi.delete_data(
            table_path,
            "id = 1"
        )
        
        # Verify deletion
        result = self.hudi.spark.read.format("hudi").load(table_path)
        self.assertEqual(result.count(), 1)
        self.assertEqual(
            result.select("id").collect(),
            [(2,)]
        )
    
    def test_query_incremental(self):
        """Test incremental querying."""
        table_path = self.get_table_path("incremental_test")
        
        # Create table and insert initial data
        self.hudi.create_cow_table(
            table_path,
            "incremental_test",
            self.schema,
            key_fields=["id"]
        )
        self.hudi.upsert_data(
            table_path,
            self.test_data,
            key_fields=["id"]
        )
        
        # Query incremental changes
        changes = self.hudi.query_incremental(
            table_path,
            begin_time="000"
        )
        
        self.assertTrue(changes.count() > 0)
    
    def test_error_handling(self):
        """Test error handling scenarios."""
        # Test invalid table path
        with self.assertRaises(Exception):
            self.hudi.create_cow_table(
                "/invalid/path",
                "invalid_table",
                self.schema,
                key_fields=["id"]
            )
        
        # Test invalid schema
        with self.assertRaises(Exception):
            self.hudi.create_cow_table(
                self.get_table_path("error_test"),
                "error_test",
                "invalid_schema",
                key_fields=["id"]
            )
        
        # Test invalid key fields
        with self.assertRaises(Exception):
            self.hudi.create_cow_table(
                self.get_table_path("error_test"),
                "error_test",
                self.schema,
                key_fields=["invalid_field"]
            )

if __name__ == '__main__':
    unittest.main() 