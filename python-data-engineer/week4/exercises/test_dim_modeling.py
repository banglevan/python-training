"""Unit tests for Dimensional Modeling."""

import unittest
import psycopg2
from dim_modeling import DimensionalModeler, Dimension, Fact

class TestDimensionalModeler(unittest.TestCase):
    """Test cases for Dimensional Modeler."""
    
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
        
        # Initialize modeler
        cls.modeler = DimensionalModeler(
            dbname='test_dwh',
            user='test_user',
            password='test_pass'
        )
        cls.modeler.connect()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.modeler.disconnect()
        
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
    
    def test_create_dimension(self):
        """Test dimension table creation."""
        # Define dimension
        product_dim = Dimension(
            name='dim_product',
            columns={
                'product_id': 'VARCHAR(50)',
                'name': 'VARCHAR(100)',
                'category': 'VARCHAR(50)',
                'price': 'DECIMAL(10,2)'
            },
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=['name', 'category', 'price']
        )
        
        # Create dimension
        self.modeler.create_dimension(product_dim)
        
        # Verify table exists
        self.modeler.cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'dim_product'
            )
        """)
        exists = self.modeler.cur.fetchone()['exists']
        self.assertTrue(exists)
        
        # Verify columns
        self.modeler.cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dim_product'
        """)
        columns = {
            r['column_name']: r['data_type']
            for r in self.modeler.cur.fetchall()
        }
        
        self.assertIn('product_key', columns)
        self.assertIn('product_id', columns)
        self.assertIn('name', columns)
        self.assertIn('category', columns)
        self.assertIn('price', columns)
    
    def test_create_fact(self):
        """Test fact table creation."""
        # Create dimensions first
        product_dim = Dimension(
            name='dim_product',
            columns={
                'product_id': 'VARCHAR(50)',
                'name': 'VARCHAR(100)'
            },
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=['name']
        )
        
        customer_dim = Dimension(
            name='dim_customer',
            columns={
                'customer_id': 'VARCHAR(50)',
                'name': 'VARCHAR(100)'
            },
            surrogate_key='customer_key',
            natural_key='customer_id',
            attributes=['name']
        )
        
        self.modeler.create_dimension(product_dim)
        self.modeler.create_dimension(customer_dim)
        
        # Define fact
        sales_fact = Fact(
            name='fact_sales',
            measures={
                'quantity': 'INTEGER',
                'amount': 'DECIMAL(12,2)'
            },
            dimensions=['product', 'customer'],
            grain=['transaction_id']
        )
        
        # Create fact
        self.modeler.create_fact(sales_fact)
        
        # Verify table exists
        self.modeler.cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'fact_sales'
            )
        """)
        exists = self.modeler.cur.fetchone()['exists']
        self.assertTrue(exists)
        
        # Verify columns and foreign keys
        self.modeler.cur.execute("""
            SELECT 
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = 'fact_sales'
            AND tc.constraint_type = 'FOREIGN KEY'
        """)
        foreign_keys = [r['column_name'] for r in self.modeler.cur.fetchall()]
        
        self.assertIn('product_key', foreign_keys)
        self.assertIn('customer_key', foreign_keys)
    
    def test_load_dimension(self):
        """Test dimension data loading."""
        # Create dimension
        product_dim = Dimension(
            name='dim_product',
            columns={
                'product_id': 'VARCHAR(50)',
                'name': 'VARCHAR(100)',
                'price': 'DECIMAL(10,2)'
            },
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=['name', 'price']
        )
        
        self.modeler.create_dimension(product_dim)
        
        # Load data
        data = [
            {
                'product_id': 'P1',
                'name': 'Product 1',
                'price': 99.99
            },
            {
                'product_id': 'P2',
                'name': 'Product 2',
                'price': 149.99
            }
        ]
        
        self.modeler.load_dimension(product_dim, data)
        
        # Verify data
        self.modeler.cur.execute(
            "SELECT COUNT(*) as count FROM dim_product"
        )
        count = self.modeler.cur.fetchone()['count']
        self.assertEqual(count, 2)
        
        self.modeler.cur.execute(
            "SELECT * FROM dim_product WHERE product_id = 'P1'"
        )
        product = self.modeler.cur.fetchone()
        self.assertEqual(product['name'], 'Product 1')
        self.assertEqual(float(product['price']), 99.99)
    
    def test_load_fact(self):
        """Test fact data loading."""
        # Create dimensions
        product_dim = Dimension(
            name='dim_product',
            columns={'product_id': 'VARCHAR(50)'},
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=[]
        )
        
        customer_dim = Dimension(
            name='dim_customer',
            columns={'customer_id': 'VARCHAR(50)'},
            surrogate_key='customer_key',
            natural_key='customer_id',
            attributes=[]
        )
        
        self.modeler.create_dimension(product_dim)
        self.modeler.create_dimension(customer_dim)
        
        # Load dimension data
        self.modeler.load_dimension(
            product_dim,
            [{'product_id': 'P1'}]
        )
        self.modeler.load_dimension(
            customer_dim,
            [{'customer_id': 'C1'}]
        )
        
        # Create and load fact
        sales_fact = Fact(
            name='fact_sales',
            measures={
                'quantity': 'INTEGER',
                'amount': 'DECIMAL(12,2)'
            },
            dimensions=['product', 'customer'],
            grain=['transaction_id']
        )
        
        self.modeler.create_fact(sales_fact)
        
        data = [
            {
                'transaction_id': 'T1',
                'product_id': 'P1',
                'customer_id': 'C1',
                'quantity': 2,
                'amount': 199.98
            }
        ]
        
        self.modeler.load_fact(sales_fact, data)
        
        # Verify data
        self.modeler.cur.execute("""
            SELECT f.*, p.product_id, c.customer_id
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_customer c ON f.customer_key = c.customer_key
        """)
        sale = self.modeler.cur.fetchone()
        
        self.assertEqual(sale['product_id'], 'P1')
        self.assertEqual(sale['customer_id'], 'C1')
        self.assertEqual(sale['quantity'], 2)
        self.assertEqual(float(sale['amount']), 199.98)
    
    def test_validate_schema(self):
        """Test schema validation."""
        # Create test schema
        product_dim = Dimension(
            name='dim_product',
            columns={'product_id': 'VARCHAR(50)'},
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=[]
        )
        
        sales_fact = Fact(
            name='fact_sales',
            measures={'amount': 'DECIMAL(12,2)'},
            dimensions=['product'],
            grain=['transaction_id']
        )
        
        self.modeler.create_dimension(product_dim)
        self.modeler.create_fact(sales_fact)
        
        # Validate schema
        issues = self.modeler.validate_schema(
            [product_dim],
            [sales_fact]
        )
        
        self.assertEqual(len(issues), 0)
        
        # Test with missing table
        bad_dim = Dimension(
            name='dim_nonexistent',
            columns={},
            surrogate_key='key',
            natural_key='id',
            attributes=[]
        )
        
        issues = self.modeler.validate_schema(
            [bad_dim],
            []
        )
        
        self.assertTrue(len(issues) > 0)
        self.assertTrue(
            any('missing' in issue.lower() for issue in issues)
        )

if __name__ == '__main__':
    unittest.main() 