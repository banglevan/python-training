"""Unit tests for Database Design."""

import unittest
import psycopg2
from db_design import DatabaseDesigner, Column, Index

class TestDatabaseDesigner(unittest.TestCase):
    """Test cases for Database Designer."""
    
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
            "DROP DATABASE IF EXISTS test_db_design"
        )
        cur.execute(
            "CREATE DATABASE test_db_design"
        )
        
        conn.close()
        
        # Initialize designer
        cls.designer = DatabaseDesigner(
            dbname='test_db_design',
            user='test_user',
            password='test_pass'
        )
        cls.designer.connect()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        cls.designer.disconnect()
        
        # Drop test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "DROP DATABASE IF EXISTS test_db_design"
        )
        conn.close()
    
    def test_create_table(self):
        """Test table creation."""
        columns = [
            Column(
                name='id',
                data_type='SERIAL',
                primary_key=True
            ),
            Column(
                name='name',
                data_type='VARCHAR(100)',
                nullable=False
            ),
            Column(
                name='email',
                data_type='VARCHAR(255)',
                unique=True
            ),
            Column(
                name='age',
                data_type='INTEGER',
                check='age >= 0'
            )
        ]
        
        self.designer.create_table('test_users', columns)
        
        # Verify table structure
        self.designer.cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'test_users'
        """)
        columns_info = self.designer.cur.fetchall()
        
        self.assertEqual(len(columns_info), 4)
        
        # Verify constraints
        self.designer.cur.execute("""
            SELECT constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'test_users'
        """)
        constraints = [r[0] for r in self.designer.cur.fetchall()]
        
        self.assertIn('PRIMARY KEY', constraints)
        self.assertIn('UNIQUE', constraints)
        self.assertIn('CHECK', constraints)
    
    def test_create_index(self):
        """Test index creation."""
        # Create test table
        columns = [
            Column(
                name='id',
                data_type='SERIAL',
                primary_key=True
            ),
            Column(
                name='name',
                data_type='VARCHAR(100)'
            ),
            Column(
                name='email',
                data_type='VARCHAR(255)'
            )
        ]
        self.designer.create_table('test_index', columns)
        
        # Create index
        index = Index(
            name='idx_test_email',
            columns=['email'],
            unique=True
        )
        self.designer.create_index('test_index', index)
        
        # Verify index
        self.designer.cur.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'test_index'
        """)
        indexes = self.designer.cur.fetchall()
        
        self.assertTrue(
            any('idx_test_email' in idx[0] for idx in indexes)
        )
        self.assertTrue(
            any('UNIQUE' in idx[1] for idx in indexes)
        )
    
    def test_analyze_table(self):
        """Test table analysis."""
        # Create and populate test table
        columns = [
            Column(
                name='id',
                data_type='SERIAL',
                primary_key=True
            ),
            Column(
                name='name',
                data_type='VARCHAR(100)'
            )
        ]
        self.designer.create_table('test_analyze', columns)
        
        self.designer.cur.execute("""
            INSERT INTO test_analyze (name)
            SELECT 'User ' || i
            FROM generate_series(1, 100) i
        """)
        self.designer.conn.commit()
        
        # Analyze table
        stats = self.designer.analyze_table('test_analyze')
        
        self.assertIn('row_count', stats)
        self.assertEqual(stats['row_count'], 100)
        self.assertIn('size', stats)
        self.assertIn('indexes', stats)
    
    def test_normalize_table(self):
        """Test table normalization."""
        # Create test table with denormalized data
        self.designer.cur.execute("""
            CREATE TABLE test_denorm (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                city VARCHAR(100),
                country VARCHAR(100),
                department VARCHAR(50),
                dept_manager VARCHAR(100)
            )
        """)
        
        # Insert test data
        self.designer.cur.execute("""
            INSERT INTO test_denorm 
                (name, city, country, department, dept_manager)
            VALUES
                ('John', 'New York', 'USA', 'IT', 'Smith'),
                ('Jane', 'London', 'UK', 'IT', 'Smith'),
                ('Bob', 'Paris', 'France', 'HR', 'Jones')
        """)
        
        # Normalize table
        column_groups = {
            'locations': ['city', 'country'],
            'departments': ['department', 'dept_manager']
        }
        
        self.designer.normalize_table(
            'test_denorm',
            column_groups
        )
        
        # Verify normalized tables
        self.designer.cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = [r[0] for r in self.designer.cur.fetchall()]
        
        self.assertIn('locations', tables)
        self.assertIn('departments', tables)
        
        # Verify foreign keys
        self.designer.cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.table_constraints
            WHERE constraint_type = 'FOREIGN KEY'
            AND table_name = 'test_denorm'
        """)
        fk_count = self.designer.cur.fetchone()[0]
        self.assertEqual(fk_count, 2)

if __name__ == '__main__':
    unittest.main() 