"""
Test query acceleration operations.
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from exercises.query_acceleration import (
    MaterializedView,
    AggregationTable,
    DynamicFilter
)

class TestQueryAcceleration(unittest.TestCase):
    """Test query acceleration functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        cls.spark = SparkSession.builder \
            .appName("TestQueryAcceleration") \
            .master("local[*]") \
            .getOrCreate()
        
        # Create test tables
        cls.spark.sql("""
            CREATE TABLE IF NOT EXISTS sales (
                id INT,
                product_id INT,
                quantity INT,
                price DOUBLE,
                sale_date DATE
            ) USING DELTA
        """)
        
        # Insert test data
        cls.spark.sql("""
            INSERT INTO sales VALUES
            (1, 1, 10, 100.0, '2023-01-01'),
            (2, 2, 20, 200.0, '2023-01-01'),
            (3, 1, 30, 300.0, '2023-01-02')
        """)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.sql("DROP TABLE IF EXISTS sales")
        cls.spark.stop()
    
    def setUp(self):
        """Set up test case."""
        self.view_manager = MaterializedView(self.spark)
        self.table_manager = AggregationTable(self.spark)
        self.filter_manager = DynamicFilter(self.spark)
    
    def test_materialized_view(self):
        """Test materialized view operations."""
        # Create view
        query = """
            SELECT product_id, SUM(quantity * price) as total_sales
            FROM sales
            GROUP BY product_id
        """
        
        self.view_manager.create_view(
            "sales_by_product",
            query,
            refresh_interval=timedelta(hours=1)
        )
        
        # Verify view creation
        self.assertIn("sales_by_product", self.view_manager.views)
        
        # Test view stats
        stats = self.view_manager.get_view_stats("sales_by_product")
        self.assertEqual(stats['query'], query)
        self.assertIn('last_refresh', stats)
        
        # Test view refresh
        self.view_manager.refresh_view(
            "sales_by_product",
            force=True
        )
        
        new_stats = self.view_manager.get_view_stats("sales_by_product")
        self.assertGreater(
            new_stats['last_refresh'],
            stats['last_refresh']
        )
    
    def test_aggregation_table(self):
        """Test aggregation table operations."""
        # Create table
        self.table_manager.create_table(
            "daily_sales",
            "sales",
            dimensions=["sale_date", "product_id"],
            measures=["quantity", "price"]
        )
        
        # Verify table creation
        self.assertIn("daily_sales", self.table_manager.tables)
        
        # Verify metadata
        metadata = self.table_manager.tables["daily_sales"]
        self.assertEqual(metadata['source'], "sales")
        self.assertEqual(
            metadata['dimensions'],
            ["sale_date", "product_id"]
        )
        
        # Test table refresh
        self.table_manager.refresh_table("daily_sales")
        
        # Verify data
        result = self.spark.sql("""
            SELECT *
            FROM daily_sales
            ORDER BY sale_date, product_id
        """)
        
        self.assertTrue(result.count() > 0)
    
    def test_dynamic_filter(self):
        """Test dynamic filter operations."""
        # Create filter
        self.filter_manager.create_filter(
            "active_products",
            "sales",
            "product_id",
            "quantity > 15",
            update_freq=timedelta(minutes=5)
        )
        
        # Verify filter creation
        self.assertIn("active_products", self.filter_manager.filters)
        
        # Test filter application
        query = "SELECT * FROM sales"
        filtered_query = self.filter_manager.apply_filter(
            "active_products",
            query
        )
        
        self.assertIn("IN (SELECT product_id", filtered_query)
        
        # Test filter update
        self.filter_manager.update_filter("active_products")
        
        metadata = self.filter_manager.filters["active_products"]
        self.assertIsNotNone(metadata['last_update'])
    
    def test_invalid_view(self):
        """Test invalid view handling."""
        with self.assertRaises(ValueError):
            self.view_manager.get_view_stats("invalid_view")
    
    def test_invalid_table(self):
        """Test invalid table handling."""
        with self.assertRaises(ValueError):
            self.table_manager.refresh_table("invalid_table")
    
    def test_invalid_filter(self):
        """Test invalid filter handling."""
        with self.assertRaises(ValueError):
            self.filter_manager.apply_filter(
                "invalid_filter",
                "SELECT * FROM sales"
            )
    
    def test_complex_query_acceleration(self):
        """Test complex query acceleration scenario."""
        # Create materialized view
        self.view_manager.create_view(
            "product_metrics",
            """
            SELECT 
                product_id,
                COUNT(*) as num_sales,
                AVG(quantity) as avg_quantity,
                SUM(price * quantity) as total_revenue
            FROM sales
            GROUP BY product_id
            """
        )
        
        # Create aggregation table
        self.table_manager.create_table(
            "daily_metrics",
            "sales",
            dimensions=["sale_date"],
            measures=["quantity", "price"],
            where_clause="quantity > 0"
        )
        
        # Create dynamic filter
        self.filter_manager.create_filter(
            "high_value_products",
            "sales",
            "product_id",
            "price * quantity > 1000"
        )
        
        # Combine all components
        base_query = """
            SELECT m.*, d.total_daily_sales
            FROM product_metrics m
            JOIN daily_metrics d ON 1=1
        """
        
        final_query = self.filter_manager.apply_filter(
            "high_value_products",
            base_query
        )
        
        # Execute query
        result = self.spark.sql(final_query)
        self.assertTrue(result.count() >= 0)

if __name__ == '__main__':
    unittest.main() 