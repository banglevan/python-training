"""Unit tests for OLAP Processing."""

import unittest
import psycopg2
from olap_proc import OLAPProcessor, Dimension, Measure

class TestOLAPProcessor(unittest.TestCase):
    """Test cases for OLAP Processor."""
    
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
        
        # Initialize processor
        cls.olap = OLAPProcessor(
            dbname='test_dwh',
            user='test_user',
            password='test_pass'
        )
        cls.olap.connect()
        
        # Create test tables
        cls.olap.cur.execute("""
            CREATE TABLE dim_product (
                product_key SERIAL PRIMARY KEY,
                product_id VARCHAR(50),
                category VARCHAR(50),
                subcategory VARCHAR(50)
            )
        """)
        
        cls.olap.cur.execute("""
            CREATE TABLE dim_time (
                time_key SERIAL PRIMARY KEY,
                date_key DATE,
                year INTEGER,
                quarter INTEGER,
                month INTEGER
            )
        """)
        
        cls.olap.cur.execute("""
            CREATE TABLE fact_sales (
                sales_key SERIAL PRIMARY KEY,
                product_key INTEGER REFERENCES dim_product,
                time_key INTEGER REFERENCES dim_time,
                quantity INTEGER,
                amount DECIMAL(12,2)
            )
        """)
        
        # Insert test data
        cls.olap.cur.execute("""
            INSERT INTO dim_product (product_id, category, subcategory)
            VALUES 
                ('P1', 'Electronics', 'Phones'),
                ('P2', 'Electronics', 'Laptops'),
                ('P3', 'Clothing', 'Shirts')
        """)
        
        cls.olap.cur.execute("""
            INSERT INTO dim_time (date_key, year, quarter, month)
            VALUES 
                ('2024-01-01', 2024, 1, 1),
                ('2024-02-01', 2024, 1, 2),
                ('2024-03-01', 2024, 1, 3)
        """)
        
        cls.olap.cur.execute("""
            INSERT INTO fact_sales (product_key, time_key, quantity, amount)
            VALUES 
                (1, 1, 10, 1000),
                (1, 2, 15, 1500),
                (2, 1, 5, 2500),
                (3, 3, 20, 800)
        """)
        
        cls.olap.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.olap.disconnect()
        
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
    
    def test_create_cube(self):
        """Test OLAP cube creation."""
        # Define dimensions
        product_dim = Dimension(
            name='product',
            hierarchies={
                'category': [
                    'category',
                    'subcategory',
                    'product_id'
                ]
            },
            attributes=['product_key']
        )
        
        time_dim = Dimension(
            name='time',
            hierarchies={
                'calendar': [
                    'year',
                    'quarter',
                    'month'
                ]
            },
            attributes=['time_key']
        )
        
        # Define measures
        measures = [
            Measure(
                name='total_sales',
                function='SUM',
                expression='amount'
            ),
            Measure(
                name='avg_quantity',
                function='AVG',
                expression='quantity'
            )
        ]
        
        # Create cube
        self.olap.create_cube(
            'sales_cube',
            'fact_sales',
            [product_dim, time_dim],
            measures
        )
        
        # Verify metadata
        self.olap.cur.execute(
            "SELECT * FROM olap_cubes WHERE cube_name = 'sales_cube'"
        )
        cube = self.olap.cur.fetchone()
        self.assertIsNotNone(cube)
        
        self.olap.cur.execute(
            "SELECT COUNT(*) as count FROM olap_dimensions WHERE cube_name = 'sales_cube'"
        )
        dim_count = self.olap.cur.fetchone()['count']
        self.assertTrue(dim_count > 0)
        
        self.olap.cur.execute(
            "SELECT COUNT(*) as count FROM olap_measures WHERE cube_name = 'sales_cube'"
        )
        measure_count = self.olap.cur.fetchone()['count']
        self.assertEqual(measure_count, 2)
    
    def test_execute_query(self):
        """Test OLAP query execution."""
        # Create cube first
        self.test_create_cube()
        
        # Execute query
        results = self.olap.execute_query(
            'sales_cube',
            ['product.category', 'time.month'],
            ['total_sales', 'avg_quantity'],
            filters={'time.year': 2024}
        )
        
        self.assertIsNotNone(results)
        self.assertTrue(len(results) > 0)
        
        # Verify aggregations
        electronics = next(
            r for r in results
            if r['category'] == 'Electronics'
        )
        self.assertTrue(float(electronics['total_sales']) > 0)
        self.assertTrue(float(electronics['avg_quantity']) > 0)
    
    def test_drill_down(self):
        """Test drill-down operation."""
        results = self.olap.drill_down(
            'sales_cube',
            'product',
            'category',
            'category',
            'subcategory',
            ['total_sales'],
            {'category': 'Electronics'}
        )
        
        self.assertIsNotNone(results)
        self.assertTrue(len(results) > 0)
        self.assertTrue(
            all('subcategory' in r for r in results)
        )
    
    def test_roll_up(self):
        """Test roll-up operation."""
        results = self.olap.roll_up(
            'sales_cube',
            'time',
            'calendar',
            'month',
            ['total_sales']
        )
        
        self.assertIsNotNone(results)
        self.assertTrue(len(results) > 0)
        self.assertTrue(
            all('quarter' in r for r in results)
        )
    
    def test_slice_and_dice(self):
        """Test slice and dice operations."""
        results = self.olap.slice_and_dice(
            'sales_cube',
            ['product.category', 'time.month'],
            ['total_sales'],
            {'time.year': 2024},  # slice
            {'product.category': 'Electronics'}  # dice
        )
        
        self.assertIsNotNone(results)
        self.assertTrue(len(results) > 0)
        self.assertTrue(
            all(
                r['category'] == 'Electronics'
                for r in results
            )
        )

if __name__ == '__main__':
    unittest.main() 