"""Unit tests for Sales DWH."""

import unittest
import psycopg2
import pandas as pd
import os
import yaml
from datetime import datetime
from sales_dwh import SalesDWH, TableConfig

class TestSalesDWH(unittest.TestCase):
    """Test cases for Sales DWH."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create test config
        os.makedirs('config', exist_ok=True)
        
        test_config = {
            'database': {
                'dbname': 'test_dwh',
                'user': 'test_user',
                'password': 'test_pass',
                'host': 'localhost',
                'port': 5432
            },
            'schema': {
                'dimensions': [
                    {
                        'name': 'dim_product',
                        'columns': {
                            'product_key': 'SERIAL PRIMARY KEY',
                            'product_id': 'VARCHAR(50)',
                            'name': 'VARCHAR(100)',
                            'category': 'VARCHAR(50)',
                            'price': 'DECIMAL(10,2)'
                        }
                    },
                    {
                        'name': 'dim_customer',
                        'columns': {
                            'customer_key': 'SERIAL PRIMARY KEY',
                            'customer_id': 'VARCHAR(50)',
                            'name': 'VARCHAR(100)',
                            'segment': 'VARCHAR(50)'
                        }
                    },
                    {
                        'name': 'dim_time',
                        'columns': {
                            'time_key': 'SERIAL PRIMARY KEY',
                            'date_key': 'DATE',
                            'year': 'INTEGER',
                            'quarter': 'INTEGER',
                            'month': 'INTEGER'
                        }
                    }
                ],
                'facts': [
                    {
                        'name': 'fact_sales',
                        'columns': {
                            'sale_key': 'SERIAL PRIMARY KEY',
                            'product_key': 'INTEGER',
                            'customer_key': 'INTEGER',
                            'time_key': 'INTEGER',
                            'quantity': 'INTEGER',
                            'amount': 'DECIMAL(12,2)'
                        },
                        'constraints': [
                            'FOREIGN KEY (product_key) REFERENCES dim_product(product_key)',
                            'FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key)',
                            'FOREIGN KEY (time_key) REFERENCES dim_time(time_key)'
                        ]
                    }
                ]
            },
            'etl': {
                'dimensions': [
                    {
                        'source': {
                            'type': 'csv',
                            'path': 'test_data/products.csv'
                        },
                        'target_table': 'dim_product',
                        'columns': [
                            'product_id',
                            'name',
                            'category',
                            'price'
                        ],
                        'mappings': {
                            'category': {
                                'ELEC': 'Electronics',
                                'CLTH': 'Clothing'
                            }
                        }
                    }
                ],
                'facts': [
                    {
                        'source': {
                            'type': 'csv',
                            'path': 'test_data/sales.csv'
                        },
                        'target_table': 'fact_sales',
                        'joins': [
                            {
                                'table': 'dim_product',
                                'columns': ['product_id']
                            }
                        ],
                        'measures': [
                            {
                                'name': 'amount',
                                'expression': 'quantity * price'
                            }
                        ]
                    }
                ]
            }
        }
        
        with open('config/dwh_config.yml', 'w') as f:
            yaml.dump(test_config, f)
        
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
        
        # Create test data directory
        os.makedirs('test_data', exist_ok=True)
        
        # Create test CSV files
        products_df = pd.DataFrame({
            'product_id': ['P1', 'P2'],
            'name': ['Product 1', 'Product 2'],
            'category': ['ELEC', 'CLTH'],
            'price': [100.0, 50.0]
        })
        products_df.to_csv(
            'test_data/products.csv',
            index=False
        )
        
        sales_df = pd.DataFrame({
            'sale_id': ['S1', 'S2'],
            'product_id': ['P1', 'P2'],
            'customer_id': ['C1', 'C1'],
            'date': ['2024-01-01', '2024-01-02'],
            'quantity': [2, 3]
        })
        sales_df.to_csv(
            'test_data/sales.csv',
            index=False
        )
        
        # Initialize DWH
        cls.dwh = SalesDWH('config/dwh_config.yml')
        cls.dwh.connect()
    
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
        
        # Remove test files
        os.remove('config/dwh_config.yml')
        os.remove('test_data/products.csv')
        os.remove('test_data/sales.csv')
        os.rmdir('test_data')
        os.rmdir('config')
    
    def test_create_schema(self):
        """Test schema creation."""
        self.dwh.create_schema()
        
        # Verify tables exist
        self.dwh.cur.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = [r['table_name'] for r in self.dwh.cur.fetchall()]
        
        self.assertIn('dim_product', tables)
        self.assertIn('dim_customer', tables)
        self.assertIn('dim_time', tables)
        self.assertIn('fact_sales', tables)
        
        # Verify foreign keys
        self.dwh.cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'FOREIGN KEY'
            AND table_name = 'fact_sales'
        """)
        fk_count = self.dwh.cur.fetchone()['count']
        self.assertEqual(fk_count, 3)
    
    def test_extract_data(self):
        """Test data extraction."""
        # Test CSV extraction
        df = self.dwh.extract_data({
            'type': 'csv',
            'path': 'test_data/products.csv'
        })
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertTrue('product_id' in df.columns)
    
    def test_transform_dimension(self):
        """Test dimension transformation."""
        df = pd.DataFrame({
            'product_id': ['P1'],
            'name': ['Product 1'],
            'category': ['ELEC'],
            'price': [100.0]
        })
        
        config = {
            'columns': [
                'product_id',
                'name',
                'category',
                'price'
            ],
            'mappings': {
                'category': {
                    'ELEC': 'Electronics'
                }
            }
        }
        
        result = self.dwh.transform_dimension(df, config)
        
        self.assertEqual(
            result.iloc[0]['category'],
            'Electronics'
        )
    
    def test_transform_fact(self):
        """Test fact transformation."""
        # Load dimension first
        products_df = pd.DataFrame({
            'product_id': ['P1'],
            'name': ['Product 1'],
            'category': ['Electronics'],
            'price': [100.0]
        })
        
        self.dwh.load_data(
            'dim_product',
            products_df
        )
        
        # Transform fact
        sales_df = pd.DataFrame({
            'sale_id': ['S1'],
            'product_id': ['P1'],
            'quantity': [2]
        })
        
        config = {
            'joins': [
                {
                    'table': 'dim_product',
                    'columns': ['product_id']
                }
            ],
            'measures': [
                {
                    'name': 'amount',
                    'expression': 'quantity * price'
                }
            ]
        }
        
        result = self.dwh.transform_fact(
            sales_df,
            config
        )
        
        self.assertTrue('product_key' in result.columns)
        self.assertEqual(
            float(result.iloc[0]['amount']),
            200.0
        )
    
    def test_load_data(self):
        """Test data loading."""
        df = pd.DataFrame({
            'product_id': ['P1'],
            'name': ['Product 1'],
            'category': ['Electronics'],
            'price': [100.0]
        })
        
        self.dwh.load_data('dim_product', df)
        
        self.dwh.cur.execute(
            "SELECT COUNT(*) as count FROM dim_product"
        )
        count = self.dwh.cur.fetchone()['count']
        self.assertEqual(count, 1)
    
    def test_create_olap_reports(self):
        """Test OLAP report creation."""
        # Load test data first
        self.dwh.run_etl_pipeline()
        
        # Create reports
        reports = self.dwh.create_olap_reports()
        
        self.assertTrue(len(reports) > 0)
        self.assertTrue(
            os.path.exists(
                'reports/sales_by_category_time.csv'
            )
        )
        self.assertTrue(
            os.path.exists(
                'reports/customer_segments.csv'
            )
        )
        
        # Clean up reports
        os.remove('reports/sales_by_category_time.csv')
        os.remove('reports/customer_segments.csv')
        os.rmdir('reports')
    
    def test_complete_pipeline(self):
        """Test complete ETL pipeline."""
        self.dwh.run_etl_pipeline()
        
        # Verify dimensions loaded
        self.dwh.cur.execute(
            "SELECT COUNT(*) as count FROM dim_product"
        )
        product_count = self.dwh.cur.fetchone()['count']
        self.assertEqual(product_count, 2)
        
        # Verify facts loaded
        self.dwh.cur.execute(
            "SELECT COUNT(*) as count FROM fact_sales"
        )
        sales_count = self.dwh.cur.fetchone()['count']
        self.assertEqual(sales_count, 2)
        
        # Verify reports created
        self.assertTrue(
            os.path.exists('reports')
        )
        
        # Clean up reports
        for f in os.listdir('reports'):
            os.remove(f'reports/{f}')
        os.rmdir('reports')

if __name__ == '__main__':
    unittest.main() 