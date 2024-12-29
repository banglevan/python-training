"""Unit tests for MySQL Performance Optimization."""

import unittest
import mysql.connector
from mysql.connector import Error
import time
from mysql_perf import MySQLPerformance, QueryMetrics

class TestMySQLPerformance(unittest.TestCase):
    """Test cases for MySQL performance features."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create test database
        conn = mysql.connector.connect(
            host='localhost',
            user='test_user',
            password='test_pass'
        )
        cursor = conn.cursor()
        
        cursor.execute(
            "DROP DATABASE IF EXISTS test_mysql"
        )
        cursor.execute(
            "CREATE DATABASE test_mysql"
        )
        conn.close()
        
        # Initialize handler
        cls.mysql = MySQLPerformance(
            database='test_mysql',
            user='test_user',
            password='test_pass'
        )
        cls.mysql.connect()
        
        # Create test tables
        cls.mysql.cur.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                status VARCHAR(20),
                created_at TIMESTAMP
            )
        """)
        
        cls.mysql.cur.execute("""
            CREATE TABLE orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                amount DECIMAL(10,2),
                status VARCHAR(20),
                order_date TIMESTAMP,
                FOREIGN KEY (customer_id) 
                REFERENCES customers(id)
            )
        """)
        
        # Insert test data
        for i in range(1000):
            cls.mysql.cur.execute("""
                INSERT INTO customers (
                    name, email, status, created_at
                ) VALUES (%s, %s, %s, NOW())
            """, (
                f"Customer {i}",
                f"customer{i}@test.com",
                'active' if i % 2 == 0 else 'inactive'
            ))
            
            if i % 3 == 0:
                cls.mysql.cur.execute("""
                    INSERT INTO orders (
                        customer_id, amount, status, order_date
                    ) VALUES (%s, %s, %s, NOW())
                """, (
                    i + 1,
                    100.00 * (i % 10),
                    'completed'
                ))
        
        cls.mysql.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.mysql.disconnect()
        
        # Drop test database
        conn = mysql.connector.connect(
            host='localhost',
            user='test_user',
            password='test_pass'
        )
        cursor = conn.cursor()
        cursor.execute(
            "DROP DATABASE IF EXISTS test_mysql"
        )
        conn.close()
    
    def test_query_analysis(self):
        """Test query performance analysis."""
        query = """
            SELECT c.*, COUNT(o.id) as order_count
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            WHERE c.status = 'active'
            GROUP BY c.id
            ORDER BY order_count DESC
            LIMIT 10
        """
        
        analysis = self.mysql.analyze_query(query)
        
        self.assertIn('plan', analysis)
        self.assertIn('profile', analysis)
        self.assertIsInstance(
            analysis['metrics'],
            QueryMetrics
        )
        
        # Verify metrics
        self.assertGreater(
            analysis['metrics'].execution_time,
            0
        )
        self.assertGreater(
            analysis['metrics'].rows_examined,
            0
        )
    
    def test_query_optimization(self):
        """Test query optimization suggestions."""
        query = """
            SELECT * 
            FROM customers c, orders o
            WHERE c.id = o.customer_id
            AND c.status = 'active'
            ORDER BY o.order_date DESC
        """
        
        optimization = self.mysql.optimize_query(
            query,
            'customers'
        )
        
        self.assertIn('current_metrics', optimization)
        self.assertIn('suggested_indexes', optimization)
        self.assertIn('query_rewrites', optimization)
        
        # Verify suggestions
        self.assertTrue(
            any('SELECT *' in rewrite 
                for rewrite in optimization['query_rewrites'])
        )
        self.assertTrue(
            'status' in optimization['suggested_indexes']
        )
    
    def test_index_impact(self):
        """Test index impact analysis."""
        # Query without index
        self.mysql.cur.execute("""
            SELECT SQL_NO_CACHE COUNT(*)
            FROM customers
            WHERE status = 'active'
        """)
        
        # Add index
        self.mysql.cur.execute("""
            CREATE INDEX idx_status
            ON customers(status)
        """)
        
        # Query with index
        query = """
            SELECT SQL_NO_CACHE COUNT(*)
            FROM customers
            WHERE status = 'active'
        """
        
        analysis = self.mysql.analyze_query(query)
        
        # Verify index usage
        self.assertIn(
            'idx_status',
            str(analysis['plan'])
        )
    
    def test_buffer_pool_tuning(self):
        """Test buffer pool tuning recommendations."""
        tuning = self.mysql.tune_buffer_pool()
        
        self.assertIn('current_settings', tuning)
        self.assertIn('pool_stats', tuning)
        self.assertIn('recommendations', tuning)
        
        # Verify recommendations
        self.assertGreater(
            tuning['recommendations']['innodb_buffer_pool_size'],
            0
        )
        self.assertGreater(
            tuning['recommendations']['innodb_buffer_pool_instances'],
            0
        )

if __name__ == '__main__':
    unittest.main() 