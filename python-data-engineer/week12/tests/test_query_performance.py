"""
Test query performance operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
from datetime import datetime
from pyspark.sql import SparkSession
from exercises.query_performance import QueryPerformance

class TestQueryPerformance(unittest.TestCase):
    """Test query performance functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestQueryPerformance") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create test tables
        cls.spark.sql("""
            CREATE TABLE IF NOT EXISTS sales (
                id INT,
                product_id INT,
                quantity INT,
                price DOUBLE
            ) USING DELTA
        """)
        
        cls.spark.sql("""
            CREATE TABLE IF NOT EXISTS products (
                id INT,
                name STRING,
                category STRING
            ) USING DELTA
        """)
        
        # Insert test data
        cls.spark.sql("""
            INSERT INTO sales VALUES
            (1, 1, 10, 100.0),
            (2, 2, 20, 200.0),
            (3, 1, 30, 300.0)
        """)
        
        cls.spark.sql("""
            INSERT INTO products VALUES
            (1, 'Product A', 'Category 1'),
            (2, 'Product B', 'Category 1'),
            (3, 'Product C', 'Category 2')
        """)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.sql("DROP TABLE IF EXISTS sales")
        cls.spark.sql("DROP TABLE IF EXISTS products")
        cls.spark.stop()
    
    def setUp(self):
        """Set up test case."""
        self.performance = QueryPerformance(self.spark)
    
    def test_query_analysis(self):
        """Test query analysis."""
        query = """
            SELECT p.category, SUM(s.quantity * s.price) as total_sales
            FROM sales s
            JOIN products p ON s.product_id = p.id
            GROUP BY p.category
        """
        
        # Analyze query
        analysis = self.performance.analyze_query(query)
        
        # Verify analysis
        self.assertIsInstance(analysis, dict)
        self.assertIn('full_table_scans', analysis)
        self.assertIn('large_shuffles', analysis)
        self.assertIn('skewed_joins', analysis)
    
    def test_query_optimization(self):
        """Test query optimization suggestions."""
        query = """
            SELECT *
            FROM sales s
            JOIN products p ON s.product_id = p.id
            WHERE p.category = 'Category 1'
        """
        
        # Get optimization suggestions
        result = self.performance.optimize_query(query)
        
        # Verify suggestions
        self.assertIn('analysis', result)
        self.assertIn('suggestions', result)
        self.assertTrue(len(result['suggestions']) > 0)
    
    def test_statistics_collection(self):
        """Test statistics collection."""
        # Collect statistics
        self.performance.collect_table_stats("sales")
        
        # Verify cache
        self.assertIn("sales", self.performance.stats_cache)
        self.assertIn('stats', self.performance.stats_cache["sales"])
        self.assertIn('timestamp', self.performance.stats_cache["sales"])
    
    def test_plan_caching(self):
        """Test plan caching."""
        query = "SELECT * FROM sales WHERE quantity > 15"
        
        # Analyze query (should cache plan)
        self.performance.analyze_query(query)
        
        # Verify cache
        cached_plan = self.performance.get_cached_plan(query)
        self.assertIsNotNone(cached_plan)
        self.assertIn('plan', cached_plan)
        self.assertIn('analysis', cached_plan)
        self.assertIn('timestamp', cached_plan)
    
    def test_cache_clearing(self):
        """Test cache clearing."""
        # Add items to cache
        query = "SELECT * FROM products"
        self.performance.analyze_query(query)
        self.performance.collect_table_stats("products")
        
        # Clear specific cache
        self.performance.clear_cache("plan")
        self.assertNotIn(query, self.performance.plan_cache)
        self.assertIn("products", self.performance.stats_cache)
        
        # Clear all cache
        self.performance.clear_cache("all")
        self.assertEqual(len(self.performance.plan_cache), 0)
        self.assertEqual(len(self.performance.stats_cache), 0)
    
    def test_complex_query_analysis(self):
        """Test analysis of complex query."""
        query = """
            WITH sales_summary AS (
                SELECT 
                    product_id,
                    SUM(quantity) as total_quantity,
                    SUM(price * quantity) as total_value
                FROM sales
                GROUP BY product_id
            )
            SELECT 
                p.category,
                COUNT(DISTINCT p.id) as num_products,
                SUM(s.total_quantity) as category_quantity,
                SUM(s.total_value) as category_value
            FROM sales_summary s
            JOIN products p ON s.product_id = p.id
            GROUP BY p.category
            HAVING SUM(s.total_value) > 1000
        """
        
        # Analyze query
        analysis = self.performance.analyze_query(query)
        
        # Verify complex analysis
        self.assertIsInstance(analysis, dict)
        suggestions = self.performance.optimize_query(query)['suggestions']
        self.assertTrue(len(suggestions) > 0)

if __name__ == '__main__':
    unittest.main() 