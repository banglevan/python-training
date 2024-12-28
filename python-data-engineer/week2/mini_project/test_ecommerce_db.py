"""Unit tests for E-commerce Database."""

import unittest
import psycopg2
import pandas as pd
import os
from ecommerce_db import EcommerceDB

class TestEcommerceDB(unittest.TestCase):
    """Test cases for E-commerce Database."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database."""
        # Create test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(
            "DROP DATABASE IF EXISTS test_ecommerce"
        )
        cur.execute(
            "CREATE DATABASE test_ecommerce"
        )
        
        conn.close()
        
        # Initialize database
        cls.db = EcommerceDB(
            dbname='test_ecommerce',
            user='test_user',
            password='test_pass'
        )
        cls.db.connect()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        cls.db.disconnect()
        
        # Drop test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "DROP DATABASE IF EXISTS test_ecommerce"
        )
        conn.close()
        
        # Clean up report files
        report_files = [
            'reports/sales_by_category.csv',
            'reports/top_customers.csv',
            'reports/product_performance.csv',
            'reports/daily_sales.csv'
        ]
        for file in report_files:
            if os.path.exists(file):
                os.remove(file)
    
    def test_create_schema(self):
        """Test schema creation."""
        self.db.create_schema()
        
        # Verify tables exist
        self.db.cur.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = [row['table_name'] for row in self.db.cur.fetchall()]
        
        expected_tables = [
            'users', 'products', 'orders', 'order_items'
        ]
        for table in expected_tables:
            self.assertIn(table, tables)
        
        # Verify indexes exist
        self.db.cur.execute("""
            SELECT indexname 
            FROM pg_indexes
            WHERE schemaname = 'public'
        """)
        indexes = [row['indexname'] for row in self.db.cur.fetchall()]
        
        expected_indexes = [
            'idx_users_email',
            'idx_products_category',
            'idx_orders_user_id',
            'idx_orders_created_at',
            'idx_order_items_order_id',
            'idx_order_items_product_id'
        ]
        for index in expected_indexes:
            self.assertIn(index, indexes)
    
    def test_generate_sample_data(self):
        """Test sample data generation."""
        self.db.create_schema()
        self.db.generate_sample_data(
            num_users=100,
            num_products=20,
            num_orders=200
        )
        
        # Check users
        self.db.cur.execute("SELECT COUNT(*) FROM users")
        user_count = self.db.cur.fetchone()['count']
        self.assertEqual(user_count, 100)
        
        # Check products
        self.db.cur.execute("SELECT COUNT(*) FROM products")
        product_count = self.db.cur.fetchone()['count']
        self.assertEqual(product_count, 20)
        
        # Check orders
        self.db.cur.execute("SELECT COUNT(*) FROM orders")
        order_count = self.db.cur.fetchone()['count']
        self.assertEqual(order_count, 200)
        
        # Check order items
        self.db.cur.execute("SELECT COUNT(*) FROM order_items")
        self.assertTrue(
            self.db.cur.fetchone()['count'] > 0
        )
    
    def test_generate_reports(self):
        """Test report generation."""
        # Setup test data
        self.db.create_schema()
        self.db.generate_sample_data(
            num_users=100,
            num_products=20,
            num_orders=200
        )
        
        # Generate reports
        reports = self.db.generate_reports()
        
        # Check report contents
        self.assertIn('sales_by_category', reports)
        self.assertIn('top_customers', reports)
        self.assertIn('product_performance', reports)
        self.assertIn('daily_sales', reports)
        
        # Verify report files
        report_files = [
            'reports/sales_by_category.csv',
            'reports/top_customers.csv',
            'reports/product_performance.csv',
            'reports/daily_sales.csv'
        ]
        
        for file in report_files:
            self.assertTrue(os.path.exists(file))
            df = pd.read_csv(file)
            self.assertTrue(len(df) > 0)
    
    def test_data_integrity(self):
        """Test data integrity constraints."""
        self.db.create_schema()
        
        # Test foreign key constraint
        with self.assertRaises(Exception):
            self.db.cur.execute("""
                INSERT INTO orders (user_id, status, total_amount)
                VALUES (999999, 'pending', 100)
            """)
        
        # Test not null constraint
        with self.assertRaises(Exception):
            self.db.cur.execute("""
                INSERT INTO users (email, name)
                VALUES (NULL, 'Test User')
            """)
        
        # Test unique constraint
        self.db.cur.execute("""
            INSERT INTO users (email, name)
            VALUES ('test@example.com', 'Test User')
        """)
        
        with self.assertRaises(Exception):
            self.db.cur.execute("""
                INSERT INTO users (email, name)
                VALUES ('test@example.com', 'Another User')
            """)

if __name__ == '__main__':
    unittest.main() 