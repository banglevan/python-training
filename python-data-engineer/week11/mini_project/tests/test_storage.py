"""
Test storage layers.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from storage.raw import RawZone
from storage.processed import ProcessedZone
from storage.curated import CuratedZone

class TestStorage(unittest.TestCase):
    """Test storage functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestStorage") \
            .master("local[*]") \
            .getOrCreate()
        
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test schema
        cls.schema = StructType([
            StructField("id", StringType(), False),
            StructField("content", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        # Create test data
        cls.test_data = cls.spark.createDataFrame([
            ("1", b"test", {"key": "value"})
        ], cls.schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
        shutil.rmtree(cls.temp_dir)
    
    def test_raw_zone(self):
        """Test raw zone operations."""
        raw = RawZone(self.spark, os.path.join(self.temp_dir, "raw"))
        
        # Test write
        raw.write_data(
            self.test_data,
            "test_table"
        )
        
        # Test read
        result = raw.read_data("test_table")
        self.assertEqual(result.count(), 1)
    
    def test_processed_zone(self):
        """Test processed zone operations."""
        processed = ProcessedZone(
            self.spark,
            os.path.join(self.temp_dir, "processed")
        )
        
        # Test write with validation
        processed.write_data(
            self.test_data,
            "test_table"
        )
        
        # Test read
        result = processed.read_data("test_table")
        self.assertEqual(result.count(), 1)
    
    def test_curated_zone(self):
        """Test curated zone operations."""
        curated = CuratedZone(
            self.spark,
            os.path.join(self.temp_dir, "curated")
        )
        
        # Test write with optimization
        curated.write_data(
            self.test_data,
            "test_table"
        )
        
        # Test read with optimization
        result = curated.read_data("test_table")
        self.assertEqual(result.count(), 1)
        
        # Test optimization
        curated.optimize_table(
            "test_table",
            ["id"]
        )

if __name__ == '__main__':
    unittest.main() 