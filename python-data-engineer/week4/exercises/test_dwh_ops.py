"""Unit tests for DWH Operations."""

import unittest
import psycopg2
from datetime import datetime, timedelta
from dwh_ops import DWHOperations, SCDType, SCDConfig

class TestDWHOperations(unittest.TestCase):
    """Test cases for DWH Operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute(
            "DROP DATABASE IF EXISTS test_dwh"
        )
        cur.execute(
            "CREATE DATABASE test_dwh"
        )
        conn.close()
        
        # Initialize DWH operations
        cls.dwh = DWHOperations(
            dbname='test_dwh',
            user='test_user',
            password='test_pass'
        )
        cls.dwh.connect()
        
        # Create test tables
        cls.dwh.cur.execute("""
            CREATE TABLE dim_product (
                product_key SERIAL PRIMARY KEY,
                product_id VARCHAR(50) UNIQUE,
                name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                effective_date TIMESTAMP,
                end_date TIMESTAMP,
                is_current BOOLEAN,
                version INTEGER
            )
        """)
        
        cls.dwh.cur.execute("""
            CREATE TABLE dim_product_history (
                history_id SERIAL PRIMARY KEY,
                product_id VARCHAR(50),
                name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                effective_date TIMESTAMP,
                end_date TIMESTAMP
            )
        """)
        
        cls.dwh.cur.execute("""
            CREATE TABLE fact_sales (
                sale_id VARCHAR(50) PRIMARY KEY,
                product_id VARCHAR(50),
                quantity INTEGER,
                amount DECIMAL(12,2),
                created_at TIMESTAMP
            )
        """)
        
        cls.dwh.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.dwh.disconnect()
        
        # Drop test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "DROP DATABASE IF EXISTS test_dwh"
        )
        conn.close()
    
    def setUp(self):
        """Set up test data."""
        self.dwh.cur.execute(
            "TRUNCATE dim_product, dim_product_history, fact_sales"
        )
        self.dwh.conn.commit()
    
    def test_scd_type1(self):
        """Test Type 1 SCD."""
        # Initial data
        product = {
            'product_id': 'P1',
            'name': 'Product 1',
            'category': 'Electronics',
            'price': 99.99
        }
        
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            SCDConfig(
                type=SCDType.TYPE1,
                track_columns=['name', 'category', 'price']
            )
        )
        
        # Update data
        product['price'] = 149.99
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            SCDConfig(
                type=SCDType.TYPE1,
                track_columns=['name', 'category', 'price']
            )
        )
        
        # Verify
        self.dwh.cur.execute("""
            SELECT * FROM dim_product
            WHERE product_id = 'P1'
        """)
        result = self.dwh.cur.fetchone()
        
        self.assertEqual(result['price'], 149.99)
        self.assertEqual(
            self.dwh.cur.rowcount,
            1
        )
    
    def test_scd_type2(self):
        """Test Type 2 SCD."""
        # Initial data
        product = {
            'product_id': 'P1',
            'name': 'Product 1',
            'category': 'Electronics',
            'price': 99.99
        }
        
        config = SCDConfig(
            type=SCDType.TYPE2,
            track_columns=['name', 'category', 'price']
        )
        
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Update data
        product['price'] = 149.99
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Verify
        self.dwh.cur.execute("""
            SELECT * FROM dim_product
            WHERE product_id = 'P1'
            ORDER BY version
        """)
        results = self.dwh.cur.fetchall()
        
        self.assertEqual(len(results), 2)
        self.assertEqual(
            results[0]['is_current'],
            False
        )
        self.assertEqual(
            results[1]['is_current'],
            True
        )
        self.assertEqual(
            results[1]['price'],
            149.99
        )
    
    def test_scd_type3(self):
        """Test Type 3 SCD."""
        # Initial data
        product = {
            'product_id': 'P1',
            'name': 'Product 1',
            'category': 'Electronics',
            'price': 99.99
        }
        
        config = SCDConfig(
            type=SCDType.TYPE3,
            track_columns=['price']
        )
        
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Update data
        product['price'] = 149.99
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Verify
        self.dwh.cur.execute("""
            SELECT * FROM dim_product
            WHERE product_id = 'P1'
        """)
        result = self.dwh.cur.fetchone()
        
        self.assertEqual(
            result['price'],
            149.99
        )
        self.assertEqual(
            result['previous_price'],
            99.99
        )
    
    def test_scd_type4(self):
        """Test Type 4 SCD."""
        # Initial data
        product = {
            'product_id': 'P1',
            'name': 'Product 1',
            'category': 'Electronics',
            'price': 99.99
        }
        
        config = SCDConfig(
            type=SCDType.TYPE4,
            track_columns=['price']
        )
        
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Update data
        product['price'] = 149.99
        self.dwh.handle_scd(
            'dim_product',
            [product],
            'product_id',
            config
        )
        
        # Verify main table
        self.dwh.cur.execute("""
            SELECT * FROM dim_product
            WHERE product_id = 'P1'
        """)
        result = self.dwh.cur.fetchone()
        
        self.assertEqual(
            result['price'],
            149.99
        )
        
        # Verify history table
        self.dwh.cur.execute("""
            SELECT * FROM dim_product_history
            WHERE product_id = 'P1'
            ORDER BY effective_date DESC
        """)
        history = self.dwh.cur.fetchall()
        
        self.assertEqual(len(history), 2)
        self.assertEqual(
            history[0]['price'],
            149.99
        )
        self.assertEqual(
            history[1]['price'],
            99.99
        )
    
    def test_incremental_load(self):
        """Test incremental load."""
        # Initial load
        now = datetime.now()
        sales = [
            {
                'sale_id': 'S1',
                'product_id': 'P1',
                'quantity': 1,
                'amount': 99.99,
                'created_at': now - timedelta(days=1)
            }
        ]
        
        self.dwh.load_incremental(
            'fact_sales',
            sales,
            'sale_id',
            'created_at'
        )
        
        # Incremental load
        new_sales = [
            {
                'sale_id': 'S2',
                'product_id': 'P1',
                'quantity': 2,
                'amount': 199.98,
                'created_at': now
            }
        ]
        
        self.dwh.load_incremental(
            'fact_sales',
            new_sales,
            'sale_id',
            'created_at'
        )
        
        # Verify
        self.dwh.cur.execute(
            "SELECT COUNT(*) as count FROM fact_sales"
        )
        result = self.dwh.cur.fetchone()
        
        self.assertEqual(result['count'], 2)

if __name__ == '__main__':
    unittest.main() 