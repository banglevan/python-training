"""
Test index optimization operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
from pyspark.sql import SparkSession
from exercises.index_optimization import (
    BTreeIndex,
    BitmapIndex,
    HashIndex,
    IndexOptimizer
)

class TestIndexOptimization(unittest.TestCase):
    """Test index optimization functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestIndexOptimization") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create test table
        cls.spark.sql("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INT,
                name STRING,
                category STRING,
                value DOUBLE
            ) USING DELTA
        """)
        
        # Insert test data
        cls.spark.sql("""
            INSERT INTO test_table VALUES
            (1, 'A', 'cat1', 10.0),
            (2, 'B', 'cat1', 20.0),
            (3, 'C', 'cat2', 30.0)
        """)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.sql("DROP TABLE IF EXISTS test_table")
        cls.spark.stop()
    
    def setUp(self):
        """Set up test case."""
        self.optimizer = IndexOptimizer(self.spark)
    
    def test_btree_index(self):
        """Test B-tree index operations."""
        index = BTreeIndex(self.spark)
        
        # Create index
        index.create("test_table", ["value"])
        
        # Verify statistics
        stats = index.get_stats()
        self.assertIn("test_table", stats)
        self.assertEqual(stats["test_table"]["type"], "btree")
        
        # Drop index
        index.drop("test_table")
        self.assertNotIn("test_table", index.get_stats())
    
    def test_bitmap_index(self):
        """Test bitmap index operations."""
        index = BitmapIndex(self.spark)
        
        # Create index
        index.create("test_table", ["category"])
        
        # Verify statistics
        stats = index.get_stats()
        self.assertIn("test_table", stats)
        self.assertEqual(stats["test_table"]["type"], "bitmap")
        
        # Drop index
        index.drop("test_table")
        self.assertNotIn("test_table", index.get_stats())
    
    def test_hash_index(self):
        """Test hash index operations."""
        index = HashIndex(self.spark)
        
        # Create index
        index.create("test_table", ["id"])
        
        # Verify statistics
        stats = index.get_stats()
        self.assertIn("test_table", stats)
        self.assertEqual(stats["test_table"]["type"], "hash")
        
        # Drop index
        index.drop("test_table")
        self.assertNotIn("test_table", index.get_stats())
    
    def test_index_suggestions(self):
        """Test index suggestions."""
        query_patterns = [
            {
                'equality_predicates': ['id'],
                'range_predicates': ['value'],
                'low_cardinality_columns': ['category']
            }
        ]
        
        suggestions = self.optimizer.suggest_indexes("test_table", query_patterns)
        
        # Verify suggestions
        self.assertTrue(len(suggestions) > 0)
        self.assertTrue(
            any(s['type'] == 'hash' for s in suggestions)
        )
        self.assertTrue(
            any(s['type'] == 'btree' for s in suggestions)
        )
        self.assertTrue(
            any(s['type'] == 'bitmap' for s in suggestions)
        )
    
    def test_invalid_index_type(self):
        """Test invalid index type handling."""
        with self.assertRaises(ValueError):
            self.optimizer.create_index(
                "invalid_type",
                "test_table",
                ["id"]
            )
    
    def test_index_stats(self):
        """Test index statistics."""
        # Create various indexes
        self.optimizer.create_index("btree", "test_table", ["value"])
        self.optimizer.create_index("hash", "test_table", ["id"])
        
        # Get all stats
        all_stats = self.optimizer.get_index_stats()
        self.assertIn("btree", all_stats)
        self.assertIn("hash", all_stats)
        
        # Get specific stats
        btree_stats = self.optimizer.get_index_stats("btree")
        self.assertIn("test_table", btree_stats)

if __name__ == '__main__':
    unittest.main() 