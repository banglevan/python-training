"""
Test storage format operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from exercises.storage_formats import ParquetOptimizer, CompressionStrategy

class TestStorageFormats(unittest.TestCase):
    """Test storage format functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestStorageFormats") \
            .master("local[*]") \
            .getOrCreate()
        
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test data
        cls.test_df = cls.spark.createDataFrame([
            (1, "text1", 1.1, [1.0] * 128),
            (2, "text2", 2.2, [2.0] * 128),
            (3, "text3", 3.3, [3.0] * 128)
        ], ["id", "text", "value", "vector"])
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
        cls.spark.stop()
    
    def setUp(self):
        """Set up test case."""
        self.optimizer = ParquetOptimizer(self.spark)
        self.test_path = os.path.join(self.temp_dir, "test.parquet")
    
    def test_schema_optimization(self):
        """Test Parquet schema optimization."""
        # Get optimized writer
        writer = self.optimizer.optimize_schema(
            self.test_df,
            compression='snappy',
            row_group_size=64 * 1024 * 1024  # 64MB
        )
        
        # Write data
        writer.save(self.test_path)
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_path))
        
        # Read metadata
        metadata = pq.read_metadata(self.test_path)
        self.assertGreater(metadata.num_row_groups, 0)
    
    def test_vector_indexing(self):
        """Test vector indexing."""
        # Write test data
        self.test_df.write.parquet(self.test_path)
        
        # Create vector index
        self.optimizer.create_vector_index(
            self.test_path,
            "vector",
            vector_dim=128
        )
        
        # Verify index file exists
        self.assertTrue(
            os.path.exists(f"{self.test_path}_vector_index")
        )
    
    def test_compression_optimization(self):
        """Test compression optimization."""
        # Define compression by column
        compression_map = {
            "id": "SNAPPY",
            "text": "ZSTD",
            "value": "GZIP"
        }
        
        # Get optimized writer
        writer = self.optimizer.optimize_compression(
            self.test_df,
            compression_map
        )
        
        # Write data
        writer.save(self.test_path)
        
        # Verify file exists
        self.assertTrue(os.path.exists(self.test_path))
    
    def test_storage_analysis(self):
        """Test storage analysis."""
        # Write test data
        self.test_df.write.parquet(self.test_path)
        
        # Analyze storage
        analysis = self.optimizer.analyze_storage(self.test_path)
        
        # Verify analysis
        self.assertIn('num_row_groups', analysis)
        self.assertIn('num_rows', analysis)
        self.assertIn('size_bytes', analysis)
        self.assertIn('compression', analysis)
        self.assertIn('encoding', analysis)
    
    def test_compression_strategy(self):
        """Test compression strategy."""
        # Test numeric column
        num_stats = {
            'dtype': 'float64',
            'unique_count': 1000,
            'total_count': 1000
        }
        self.assertEqual(
            CompressionStrategy.suggest_compression(num_stats),
            'ZSTD'
        )
        
        # Test string column
        str_stats = {
            'dtype': 'string',
            'unique_count': 100,
            'total_count': 1000
        }
        self.assertEqual(
            CompressionStrategy.suggest_compression(str_stats),
            'DICTIONARY,ZSTD'
        )
    
    def test_compression_savings(self):
        """Test compression savings estimation."""
        # Create sample data
        sample_data = pa.Table.from_arrays(
            [pa.array(range(1000))],
            names=['column']
        )
        
        # Estimate savings
        savings = CompressionStrategy.estimate_savings(
            1000000,  # 1MB
            'SNAPPY',
            sample_data
        )
        
        # Verify estimates
        self.assertIn('original_size', savings)
        self.assertIn('compressed_size', savings)
        self.assertIn('ratio', savings)
        self.assertIn('savings_bytes', savings)
        self.assertIn('savings_percent', savings)

if __name__ == '__main__':
    unittest.main() 