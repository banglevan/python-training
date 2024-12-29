"""
Unit Tests for InfluxDB Metrics
------------------------------

Test Coverage:
1. Data Writing
   - Batch operations
   - Point formatting
   - Write performance

2. Query Operations
   - Time range queries
   - Aggregations
   - Continuous queries

3. Retention Management
   - Policy creation
   - Data lifecycle
   - Storage optimization
"""

import unittest
from influx_metrics import InfluxMetrics
from datetime import datetime, timedelta
import time
import random

class TestInfluxMetrics(unittest.TestCase):
    """Test cases for InfluxDB metrics."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.influx = InfluxMetrics(
            bucket='test_metrics'
        )
        
        # Create test bucket if needed
        cls.influx.client.buckets_api().create_bucket(
            bucket_name='test_metrics',
            org=cls.influx.org
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Delete test bucket
        cls.influx.client.buckets_api().delete_bucket(
            bucket='test_metrics'
        )
        cls.influx.close()
    
    def test_write_batch(self):
        """Test batch write operations."""
        # Generate test data
        points = [
            {
                'cpu_usage': random.uniform(0, 100),
                'memory_used': random.uniform(0, 16),
                'host': f'server{i % 3}'
            }
            for i in range(1000)
        ]
        
        # Test different batch sizes
        batch_sizes = [100, 500]
        metrics = {}
        
        for size in batch_sizes:
            start_time = time.time()
            self.influx.write_batch(
                'system_metrics',
                points,
                tags={'env': 'test'},
                batch_size=size
            )
            duration = time.time() - start_time
            metrics[size] = duration
        
        # Verify write performance
        self.assertLess(
            metrics[500],
            metrics[100],
            "Larger batch size should be faster"
        )
        
        # Verify data written
        result = self.influx.query_metrics(
            'SELECT count(*) FROM system_metrics'
        )
        self.assertEqual(
            len(result),
            1000
        )
    
    def test_continuous_query(self):
        """Test continuous query creation and execution."""
        # Write test data
        points = [
            {
                'temperature': random.uniform(20, 30),
                'humidity': random.uniform(40, 60),
                'location': f'room{i % 5}'
            }
            for i in range(100)
        ]
        
        self.influx.write_batch(
            'sensor_data',
            points
        )
        
        # Create continuous query
        self.influx.create_continuous_query(
            'temp_hourly',
            """
            SELECT mean(temperature) as temp_avg,
                   max(temperature) as temp_max
            FROM sensor_data
            GROUP BY time(1h), location
            """
        )
        
        # Wait for CQ execution
        time.sleep(2)
        
        # Verify aggregated data
        results = self.influx.query_metrics(
            'SELECT * FROM temp_hourly ORDER BY time DESC LIMIT 5'
        )
        
        self.assertTrue(len(results) > 0)
        self.assertIn('temp_avg', results[0])
        self.assertIn('temp_max', results[0])
    
    def test_retention_policy(self):
        """Test retention policy management."""
        # Create retention policies
        policies = [
            ('short_term', '1d'),
            ('long_term', '30d')
        ]
        
        for name, duration in policies:
            self.influx.create_retention_policy(
                name,
                duration,
                replication=1
            )
        
        # Write data with different policies
        for name, _ in policies:
            self.influx.write_batch(
                'test_metrics',
                [{'value': random.random()}],
                tags={'retention': name}
            )
        
        # Verify policies
        results = self.influx.query_metrics(
            'SHOW RETENTION POLICIES'
        )
        
        policy_names = [r['name'] for r in results]
        self.assertTrue(
            all(name in policy_names 
                for name, _ in policies)
        )
    
    def test_query_performance(self):
        """Test query performance and optimization."""
        # Write test data with timestamps
        now = datetime.utcnow()
        points = []
        
        for i in range(1000):
            points.append({
                'value': random.random(),
                'timestamp': now - timedelta(minutes=i),
                'category': f'cat_{i % 5}'
            })
        
        self.influx.write_batch(
            'perf_test',
            points
        )
        
        # Test different query patterns
        queries = [
            # Simple range query
            """
            SELECT * FROM perf_test
            WHERE time > now() - 1h
            """,
            # Aggregation query
            """
            SELECT mean(value)
            FROM perf_test
            WHERE time > now() - 1h
            GROUP BY time(5m), category
            """,
            # Complex condition
            """
            SELECT count(*)
            FROM perf_test
            WHERE value > 0.5
            AND category IN ('cat_1', 'cat_2')
            GROUP BY category
            """
        ]
        
        for query in queries:
            start_time = time.time()
            results = self.influx.query_metrics(query)
            duration = time.time() - start_time
            
            self.assertIsNotNone(results)
            self.assertLess(
                duration,
                1.0,
                f"Query took too long: {duration:.2f}s"
            )

if __name__ == '__main__':
    unittest.main() 