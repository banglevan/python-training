"""Unit tests for Query Performance."""

import unittest
import psycopg2
import json
import os
from query_perf import QueryOptimizer, QueryPlan

class TestQueryOptimizer(unittest.TestCase):
    """Test cases for Query Optimizer."""
    
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
        
        # Create test database
        cur.execute(
            "DROP DATABASE IF EXISTS test_query_perf"
        )
        cur.execute(
            "CREATE DATABASE test_query_perf"
        )
        
        conn.close()
        
        # Setup test tables
        cls.optimizer = QueryOptimizer(
            dbname='test_query_perf',
            user='test_user',
            password='test_pass'
        )
        cls.optimizer.connect()
        
        # Create sample tables
        cls.optimizer.cur.execute("""
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255)
            )
        """)
        
        cls.optimizer.cur.execute("""
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                amount DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert sample data
        cls.optimizer.cur.execute("""
            INSERT INTO users (name, email)
            SELECT 
                'User ' || i,
                'user' || i || '@example.com'
            FROM generate_series(1, 1000) i
        """)
        
        cls.optimizer.cur.execute("""
            INSERT INTO orders (user_id, amount)
            SELECT 
                floor(random() * 1000 + 1)::int,
                random() * 1000
            FROM generate_series(1, 5000)
        """)
        
        cls.optimizer.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        cls.optimizer.disconnect()
        
        # Drop test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "DROP DATABASE IF EXISTS test_query_perf"
        )
        conn.close()
    
    def setUp(self):
        """Set up test case."""
        self.test_query = """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY order_count DESC
        """
    
    def test_analyze_query(self):
        """Test query analysis."""
        plan = self.optimizer.analyze_query(self.test_query)
        
        self.assertIsInstance(plan, QueryPlan)
        self.assertEqual(plan.query, self.test_query)
        self.assertGreater(plan.execution_time, 0)
        self.assertGreater(plan.rows_affected, 0)
        self.assertIsInstance(plan.plan, dict)
    
    def test_benchmark_query(self):
        """Test query benchmarking."""
        results = self.optimizer.benchmark_query(
            self.test_query,
            iterations=3
        )
        
        self.assertIn('min_time', results)
        self.assertIn('max_time', results)
        self.assertIn('avg_time', results)
        self.assertEqual(results['iterations'], 3)
        self.assertGreater(results['avg_time'], 0)
    
    def test_suggest_optimizations(self):
        """Test optimization suggestions."""
        suggestions = self.optimizer.suggest_optimizations(
            self.test_query
        )
        
        self.assertIsInstance(suggestions, list)
        self.assertTrue(len(suggestions) > 0)
        self.assertTrue(
            any('index' in s.lower() for s in suggestions)
        )
    
    def test_create_performance_report(self):
        """Test performance report generation."""
        # Analyze query first
        self.optimizer.analyze_query(self.test_query)
        
        report_file = 'test_performance_report.json'
        self.optimizer.create_performance_report(report_file)
        
        self.assertTrue(os.path.exists(report_file))
        
        with open(report_file) as f:
            report = json.load(f)
            self.assertIn('timestamp', report)
            self.assertIn('queries', report)
            self.assertTrue(len(report['queries']) > 0)
        
        # Clean up
        os.remove(report_file)
    
    def test_extract_indexes_from_plan(self):
        """Test index extraction from plan."""
        plan = {
            'Plan': {
                'Node Type': 'Index Scan',
                'Index Name': 'test_index',
                'Plans': [
                    {
                        'Node Type': 'Index Scan',
                        'Index Name': 'another_index'
                    }
                ]
            }
        }
        
        indexes = self.optimizer._extract_indexes_from_plan([plan])
        self.assertEqual(len(indexes), 2)
        self.assertIn('test_index', indexes)
        self.assertIn('another_index', indexes)

if __name__ == '__main__':
    unittest.main() 