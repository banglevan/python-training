"""Unit tests for SQL Operations."""

import unittest
import psycopg2
from datetime import datetime
from sql_ops import DatabaseOperations

class TestDatabaseOperations(unittest.TestCase):
    """Test cases for Database Operations."""
    
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
            "DROP DATABASE IF EXISTS test_sql_ops"
        )
        cur.execute(
            "CREATE DATABASE test_sql_ops"
        )
        
        conn.close()
        
        # Initialize database
        cls.db = DatabaseOperations(
            dbname='test_sql_ops',
            user='test_user',
            password='test_pass'
        )
        cls.db.connect()
        
        # Create test table
        cls.db.cur.execute("""
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255) UNIQUE,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cls.db.conn.commit()
    
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
            "DROP DATABASE IF EXISTS test_sql_ops"
        )
        conn.close()
    
    def setUp(self):
        """Set up test case."""
        # Clear test table
        self.db.cur.execute("TRUNCATE test_users RESTART IDENTITY")
        self.db.conn.commit()
    
    def test_create_record(self):
        """Test record creation."""
        data = {
            'name': 'John Doe',
            'email': 'john@example.com',
            'age': 30
        }
        
        record_id = self.db.create_record('test_users', data)
        
        self.assertIsNotNone(record_id)
        self.assertEqual(record_id, 1)
        
        # Verify record
        self.db.cur.execute(
            "SELECT * FROM test_users WHERE id = %s",
            (record_id,)
        )
        record = self.db.cur.fetchone()
        
        self.assertEqual(record['name'], data['name'])
        self.assertEqual(record['email'], data['email'])
        self.assertEqual(record['age'], data['age'])
    
    def test_read_records(self):
        """Test record reading."""
        # Insert test data
        test_data = [
            {'name': 'John', 'email': 'john@test.com', 'age': 30},
            {'name': 'Jane', 'email': 'jane@test.com', 'age': 25},
            {'name': 'Bob', 'email': 'bob@test.com', 'age': 35}
        ]
        
        for data in test_data:
            self.db.create_record('test_users', data)
        
        # Test basic read
        records = self.db.read_records('test_users')
        self.assertEqual(len(records), 3)
        
        # Test with columns
        records = self.db.read_records(
            'test_users',
            columns=['name', 'age']
        )
        self.assertIn('name', records[0])
        self.assertIn('age', records[0])
        self.assertNotIn('email', records[0])
        
        # Test with conditions
        records = self.db.read_records(
            'test_users',
            conditions={'age': 30}
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['name'], 'John')
        
        # Test with order and limit
        records = self.db.read_records(
            'test_users',
            order_by='age DESC',
            limit=2
        )
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]['name'], 'Bob')
    
    def test_update_record(self):
        """Test record update."""
        # Create test record
        data = {
            'name': 'John',
            'email': 'john@test.com',
            'age': 30
        }
        record_id = self.db.create_record('test_users', data)
        
        # Update record
        update_data = {
            'name': 'John Smith',
            'age': 31
        }
        self.db.update_record(
            'test_users',
            record_id,
            update_data
        )
        
        # Verify update
        self.db.cur.execute(
            "SELECT * FROM test_users WHERE id = %s",
            (record_id,)
        )
        record = self.db.cur.fetchone()
        
        self.assertEqual(record['name'], update_data['name'])
        self.assertEqual(record['age'], update_data['age'])
        self.assertEqual(record['email'], data['email'])
    
    def test_delete_record(self):
        """Test record deletion."""
        # Create test record
        data = {
            'name': 'John',
            'email': 'john@test.com',
            'age': 30
        }
        record_id = self.db.create_record('test_users', data)
        
        # Delete record
        self.db.delete_record('test_users', record_id)
        
        # Verify deletion
        self.db.cur.execute(
            "SELECT * FROM test_users WHERE id = %s",
            (record_id,)
        )
        record = self.db.cur.fetchone()
        self.assertIsNone(record)
    
    def test_execute_join(self):
        """Test join execution."""
        # Create additional test table
        self.db.cur.execute("""
            CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES test_users(id),
                amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        user_data = {
            'name': 'John',
            'email': 'john@test.com',
            'age': 30
        }
        user_id = self.db.create_record('test_users', user_data)
        
        self.db.cur.execute("""
            INSERT INTO test_orders (user_id, amount)
            VALUES (%s, %s)
        """, (user_id, 100.00))
        
        # Test join
        results = self.db.execute_join(
            tables=['test_users', 'test_orders'],
            join_conditions=[
                'test_users.id = test_orders.user_id'
            ],
            columns=[
                'test_users.name',
                'test_orders.amount'
            ]
        )
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['name'], 'John')
        self.assertEqual(float(results[0]['amount']), 100.00)
    
    def test_execute_aggregation(self):
        """Test aggregation execution."""
        # Insert test data
        test_data = [
            {'name': 'John', 'email': 'john@test.com', 'age': 30},
            {'name': 'Jane', 'email': 'jane@test.com', 'age': 30},
            {'name': 'Bob', 'email': 'bob@test.com', 'age': 35}
        ]
        
        for data in test_data:
            self.db.create_record('test_users', data)
        
        # Test aggregation
        results = self.db.execute_aggregation(
            table='test_users',
            group_by=['age'],
            aggregations={
                'id': 'COUNT',
                'age': 'MAX'
            }
        )
        
        self.assertEqual(len(results), 2)  # Two age groups
        age_30_group = next(
            r for r in results if r['age'] == 30
        )
        self.assertEqual(age_30_group['count'], 2)

if __name__ == '__main__':
    unittest.main() 