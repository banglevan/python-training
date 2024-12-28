"""Unit tests for Spark Basics."""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from spark_basics import SparkBasics

class TestSparkBasics(unittest.TestCase):
    """Test cases for Spark Basics."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.spark_basics = SparkBasics("TestSparkBasics")
        
        # Sample data for testing
        cls.rdd_data = [
            ("product1", 100),
            ("product2", 200),
            ("product3", 150)
        ]
        
        cls.df_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("amount", IntegerType(), True),
            StructField("category", StringType(), True)
        ])
        
        cls.df_data = [
            {"product_id": "P1", "amount": 100, "category": "A"},
            {"product_id": "P2", "amount": 200, "category": "B"},
            {"product_id": "P3", "amount": 150, "category": "A"}
        ]
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.spark_basics:
            cls.spark_basics.cleanup()
    
    def test_rdd_operations(self):
        """Test RDD operations."""
        results = self.spark_basics.rdd_operations(
            self.rdd_data
        )
        
        self.assertIsInstance(results, dict)
        self.assertIn('original', results)
        self.assertIn('mapped', results)
        self.assertIn('filtered', results)
        
        # Verify transformations
        self.assertEqual(
            len(results['original']),
            len(self.rdd_data)
        )
        self.assertTrue(
            all(isinstance(x[1], int) 
                for x in results['mapped'])
        )
    
    def test_dataframe_operations(self):
        """Test DataFrame operations."""
        results = self.spark_basics.dataframe_operations(
            self.df_data,
            self.df_schema
        )
        
        self.assertIsInstance(results, dict)
        self.assertIn('original', results)
        self.assertIn('selected', results)
        self.assertIn('filtered', results)
        self.assertIn('aggregated', results)
        
        # Verify transformations
        self.assertEqual(
            results['original'].count(),
            len(self.df_data)
        )
        self.assertTrue(
            len(results['selected'].columns) <= 3
        )
        self.assertTrue(
            'row_count' in results['aggregated'].columns
        )
    
    def test_sql_operations(self):
        """Test SQL operations."""
        # Create test DataFrame
        df = self.spark_basics.dataframe_operations(
            self.df_data,
            self.df_schema
        )['original']
        
        results = self.spark_basics.sql_operations(
            {"test_table": df}
        )
        
        self.assertIsInstance(results, dict)
        self.assertIn('basic_select', results)
        self.assertIn('aggregation', results)
        
        # Verify queries
        self.assertTrue(
            results['basic_select'].count() <= 5
        )
        self.assertEqual(
            results['aggregation'].count(),
            1
        )
    
    def test_error_handling(self):
        """Test error handling."""
        # Test with invalid RDD data
        with self.assertRaises(Exception):
            self.spark_basics.rdd_operations(None)
        
        # Test with invalid DataFrame schema
        with self.assertRaises(Exception):
            self.spark_basics.dataframe_operations(
                self.df_data,
                None
            )
        
        # Test with invalid SQL query
        with self.assertRaises(Exception):
            self.spark_basics.sql_operations({})

if __name__ == '__main__':
    unittest.main() 