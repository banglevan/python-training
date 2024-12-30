"""
Unit Tests for TimescaleDB Analytics
----------------------------------

Test Coverage:
1. Hypertable Operations
   - Table creation
   - Chunk management
   - Compression settings

2. Continuous Aggregates
   - View creation
   - Refresh policies
   - Query optimization

3. Data Management
   - Retention policies
   - Compression
   - Performance analysis
"""

import unittest
from timescale_analytics import TimescaleAnalytics
from datetime import datetime, timedelta
import random
import time

class TestTimescaleAnalytics(unittest.TestCase):
    """Test cases for TimescaleDB analytics."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.tsdb = TimescaleAnalytics(
            dbname='test_timescale'
        )
        
        # Create test table
        cls.tsdb.cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                time TIMESTAMPTZ NOT NULL,
                sensor_id TEXT NOT NULL,
                temperature DOUBLE PRECISION,
                humidity DOUBLE PRECISION
            )
        """)
        cls.tsdb.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.tsdb.cur.execute("""
            DROP TABLE IF EXISTS sensor_data CASCADE
        """)
        cls.tsdb.conn.commit()
        cls.tsdb.close()
    
    def test_hypertable_creation(self):
        """Test hypertable creation and configuration."""
        # Create hypertable
        self.tsdb.create_hypertable(
            'sensor_data',
            'time',
            chunk_interval='1 day',
            dimensions=['sensor_id']
        )
        
        # Verify hypertable
        self.tsdb.cur.execute("""
            SELECT * FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'sensor_data'
        """)
        result = self.tsdb.cur.fetchone()
        
        self.assertIsNotNone(result)
        self.assertEqual(
            result['chunk_time_interval'],
            timedelta(days=1)
        )
        
        # Check compression settings
        self.tsdb.cur.execute("""
            SELECT compression_enabled
            FROM timescaledb_information.hypertables
            WHERE hypertable_name = 'sensor_data'
        """)
        self.assertTrue(
            self.tsdb.cur.fetchone()['compression_enabled']
        )
    
    def test_continuous_aggregate(self):
        """Test continuous aggregate creation and refresh."""
        # Insert test data
        now = datetime.utcnow()
        for i in range(1000):
            self.tsdb.cur.execute("""
                INSERT INTO sensor_data (
                    time, sensor_id, temperature, humidity
                ) VALUES (%s, %s, %s, %s)
            """, (
                now - timedelta(minutes=i),
                f'sensor_{i % 5}',
                random.uniform(20, 30),
                random.uniform(40, 60)
            ))
        self.tsdb.conn.commit()
        
        # Create continuous aggregate
        self.tsdb.create_continuous_aggregate(
            'hourly_stats',
            """
            SELECT time_bucket('1 hour', time) as bucket,
                   sensor_id,
                   avg(temperature) as temp_avg,
                   avg(humidity) as humid_avg
            FROM sensor_data
            GROUP BY 1, 2
            """
        )
        
        # Verify materialized data
        self.tsdb.cur.execute("""
            SELECT count(*) as count
            FROM hourly_stats
        """)
        count = self.tsdb.cur.fetchone()['count']
        self.assertGreater(count, 0)
        
        # Test query performance
        start_time = time.time()
        self.tsdb.cur.execute("""
            SELECT bucket, avg(temp_avg)
            FROM hourly_stats
            GROUP BY 1
            ORDER BY 1
        """)
        duration = time.time() - start_time
        
        self.assertLess(
            duration,
            0.1,
            "Materialized view query should be fast"
        )
    
    def test_compression_policy(self):
        """Test chunk compression and management."""
        # Insert historical data
        old_time = datetime.utcnow() - timedelta(days=10)
        for i in range(100):
            self.tsdb.cur.execute("""
                INSERT INTO sensor_data (
                    time, sensor_id, temperature, humidity
                ) VALUES (%s, %s, %s, %s)
            """, (
                old_time + timedelta(hours=i),
                'old_sensor',
                random.uniform(20, 30),
                random.uniform(40, 60)
            ))
        self.tsdb.conn.commit()
        
        # Compress old chunks
        self.tsdb.compress_chunks(
            'sensor_data',
            '7 days'
        )
        
        # Verify compression
        self.tsdb.cur.execute("""
            SELECT count(*) as count
            FROM timescaledb_information.compressed_chunks
            WHERE hypertable_name = 'sensor_data'
        """)
        compressed_count = self.tsdb.cur.fetchone()['count']
        self.assertGreater(compressed_count, 0)
    
    def test_query_analysis(self):
        """Test query analysis and optimization."""
        # Generate query plan
        analysis = self.tsdb.analyze_query(
            """
            SELECT time_bucket('15 minutes', time) as bucket,
                   sensor_id,
                   avg(temperature) as temp_avg
            FROM sensor_data
            WHERE time >= %s
            GROUP BY 1, 2
            ORDER BY 1 DESC
            LIMIT 100
            """,
            (datetime.utcnow() - timedelta(days=1),)
        )
        
        # Verify analysis
        self.assertIn('plan', analysis)
        self.assertIn('duration', analysis)
        
        # Check execution time
        duration = float(
            analysis['duration'].rstrip('s')
        )
        self.assertLess(
            duration,
            1.0,
            "Query should complete within 1 second"
        )
        
        # Verify chunk exclusion
        plan_text = str(analysis['plan'])
        self.assertIn(
            'Chunks excluded',
            plan_text,
            "Query should use chunk exclusion"
        )

if __name__ == '__main__':
    unittest.main() 