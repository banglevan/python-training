"""Unit tests for Advanced PostgreSQL Operations."""

import unittest
import psycopg2
from datetime import datetime, timedelta
import time
from postgres_advanced import PostgresAdvanced

class TestPostgresAdvanced(unittest.TestCase):
    """Test cases for PostgreSQL advanced features."""
    
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
            "DROP DATABASE IF EXISTS test_pg"
        )
        cur.execute(
            "CREATE DATABASE test_pg"
        )
        conn.close()
        
        # Initialize handler
        cls.pg = PostgresAdvanced(
            dbname='test_pg',
            user='test_user',
            password='test_pass'
        )
        cls.pg.connect()
        
        # Create test tables
        cls.pg.cur.execute("""
            CREATE TABLE test_data (
                id SERIAL PRIMARY KEY,
                created_at TIMESTAMP,
                category VARCHAR(50),
                data JSONB
            )
        """)
        
        # Insert test data
        now = datetime.now()
        for i in range(1000):
            cls.pg.cur.execute("""
                INSERT INTO test_data (
                    created_at, 
                    category,
                    data
                ) VALUES (%s, %s, %s)
            """, (
                now - timedelta(days=i),
                f"cat_{i % 5}",
                {'value': i}
            ))
        
        cls.pg.conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.pg.disconnect()
        
        # Drop test database
        conn = psycopg2.connect(
            dbname='postgres',
            user='test_user',
            password='test_pass'
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            "DROP DATABASE IF EXISTS test_pg"
        )
        conn.close()
    
    def test_range_partitioning(self):
        """Test range-based partitioning."""
        # Setup partitioning
        ranges = [
            {
                'suffix': '2024_q1',
                'start': '2024-01-01',
                'end': '2024-04-01'
            },
            {
                'suffix': '2024_q2',
                'start': '2024-04-01',
                'end': '2024-07-01'
            }
        ]
        
        self.pg.setup_partitioning(
            'events',
            'created_at',
            'range',
            ranges=ranges
        )
        
        # Verify partitions
        self.pg.cur.execute("""
            SELECT tablename 
            FROM pg_tables
            WHERE tablename LIKE 'events%'
        """)
        partitions = self.pg.cur.fetchall()
        
        self.assertEqual(len(partitions), 3)  # Parent + 2 partitions
        
        # Test data insertion
        self.pg.cur.execute("""
            INSERT INTO events (created_at, data)
            VALUES ('2024-02-01', '{"test": true}')
        """)
        
        # Verify partition routing
        self.pg.cur.execute("""
            SELECT COUNT(*) as count
            FROM events_2024_q1
        """)
        count = self.pg.cur.fetchone()['count']
        self.assertEqual(count, 1)
    
    def test_list_partitioning(self):
        """Test list-based partitioning."""
        categories = ['A', 'B', 'C']
        
        self.pg.setup_partitioning(
            'category_data',
            'category',
            'list',
            list_values=categories
        )
        
        # Verify partitions
        self.pg.cur.execute("""
            SELECT tablename 
            FROM pg_tables
            WHERE tablename LIKE 'category_data%'
        """)
        partitions = self.pg.cur.fetchall()
        
        self.assertEqual(len(partitions), 4)  # Parent + 3 partitions
    
    def test_replication_setup(self):
        """Test replication configuration."""
        self.pg.setup_replication(
            'streaming',
            master_config={
                'slot_name': 'test_slot',
                'create_slot': True
            }
        )
        
        # Verify replication slot
        self.pg.cur.execute("""
            SELECT slot_name
            FROM pg_replication_slots
            WHERE slot_name = 'test_slot'
        """)
        slot = self.pg.cur.fetchone()
        
        self.assertIsNotNone(slot)
    
    def test_vacuum_performance(self):
        """Test VACUUM performance."""
        # Create test conditions
        self.pg.cur.execute("""
            DELETE FROM test_data
            WHERE id % 2 = 0
        """)
        
        # Measure before state
        self.pg.cur.execute("""
            SELECT n_dead_tup
            FROM pg_stat_user_tables
            WHERE relname = 'test_data'
        """)
        before_dead_tuples = self.pg.cur.fetchone()['n_dead_tup']
        
        # Perform VACUUM
        results = self.pg.perform_vacuum(
            'test_data',
            vacuum_type='FULL',
            analyze=True
        )
        
        # Verify cleanup
        self.assertTrue(
            results['after']['dead_tuples'] < before_dead_tuples
        )
        self.assertLess(
            float(results['duration'].rstrip('s')),
            10.0  # Should complete within 10 seconds
        )
    
    def test_maintenance_monitoring(self):
        """Test maintenance monitoring."""
        stats = self.pg.monitor_maintenance()
        
        self.assertIn('autovacuum_settings', stats)
        self.assertIn('table_stats', stats)
        self.assertIn('bloat_stats', stats)
        
        # Verify monitoring data
        table_stats = next(
            stat for stat in stats['table_stats']
            if stat['table_name'] == 'test_data'
        )
        
        self.assertIsNotNone(table_stats['live_tuples'])
        self.assertIsNotNone(table_stats['dead_tuples'])

if __name__ == '__main__':
    unittest.main() 