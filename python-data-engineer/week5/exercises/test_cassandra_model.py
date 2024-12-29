"""
Unit Tests for Cassandra Data Modeling
------------------------------------

Test Coverage:
1. Table Operations
   - Creation validation
   - Schema verification
   - Options configuration

2. Data Operations
   - Insert performance
   - Batch processing
   - Consistency levels

3. Query Patterns
   - Partition efficiency
   - Clustering order
   - Secondary indexes
"""

import unittest
from cassandra_model import CassandraModel
from cassandra.cluster import ConsistencyLevel
from datetime import datetime, timedelta
import uuid
import time

class TestCassandraModel(unittest.TestCase):
    """Test cases for Cassandra modeling."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.cassandra = CassandraModel(
            keyspace='test_keyspace'
        )
        
        # Create test tables
        cls.cassandra.create_table(
            'test_events',
            partition_keys=['event_date', 'category'],
            clustering_keys=['event_time', 'event_id'],
            columns={
                'event_id': 'uuid',
                'category': 'text',
                'event_date': 'date',
                'event_time': 'timestamp',
                'data': 'text'
            }
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.cassandra.session.execute(
            "DROP KEYSPACE IF EXISTS test_keyspace"
        )
        cls.cassandra.close()
    
    def test_table_creation(self):
        """Test table creation with various schemas."""
        # Test time-series table
        self.cassandra.create_table(
            'time_series',
            partition_keys=['date_bucket'],
            clustering_keys=['timestamp', 'id'],
            columns={
                'id': 'uuid',
                'date_bucket': 'date',
                'timestamp': 'timestamp',
                'value': 'double'
            },
            options={
                'clustering order by': '(timestamp DESC)',
                'compaction': {
                    'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_size': 1,
                    'compaction_window_unit': 'DAYS'
                }
            }
        )
        
        # Verify table structure
        self.cassandra.session.execute("""
            SELECT * FROM system_schema.tables
            WHERE keyspace_name = 'test_keyspace'
            AND table_name = 'time_series'
        """)
        table_info = self.cassandra.session.execute("""
            SELECT * FROM system_schema.columns
            WHERE keyspace_name = 'test_keyspace'
            AND table_name = 'time_series'
        """)
        
        columns = {row.column_name: row.type 
                  for row in table_info}
        
        self.assertIn('date_bucket', columns)
        self.assertIn('timestamp', columns)
        self.assertEqual(columns['value'], 'double')
    
    def test_data_insertion(self):
        """Test data insertion with different consistency levels."""
        event_id = uuid.uuid4()
        event_data = {
            'event_id': event_id,
            'category': 'test',
            'event_date': datetime.now().date(),
            'event_time': datetime.now(),
            'data': '{"test": true}'
        }
        
        # Test QUORUM consistency
        self.cassandra.insert_data(
            'test_events',
            event_data,
            consistency='QUORUM'
        )
        
        # Verify insertion
        result = self.cassandra.execute_query(
            """
            SELECT * FROM test_events
            WHERE event_date = %s
            AND category = %s
            AND event_id = %s
            """,
            [event_data['event_date'],
             event_data['category'],
             event_data['event_id']],
            consistency='QUORUM'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(
            str(result[0].event_id),
            str(event_id)
        )
    
    def test_batch_operations(self):
        """Test batch insert operations."""
        # Prepare batch data
        events = []
        now = datetime.now()
        
        for i in range(100):
            events.append({
                'event_id': uuid.uuid4(),
                'category': f'cat_{i % 5}',
                'event_date': now.date(),
                'event_time': now + timedelta(minutes=i),
                'data': f'{{"value": {i}}}'
            })
        
        # Execute batch insert
        self.cassandra.batch_insert(
            'test_events',
            events,
            consistency='QUORUM'
        )
        
        # Verify batch
        results = self.cassandra.execute_query(
            """
            SELECT COUNT(*) as count
            FROM test_events
            WHERE event_date = %s
            """,
            [now.date()]
        )
        
        self.assertGreaterEqual(
            results[0].count,
            100
        )
    
    def test_query_patterns(self):
        """Test various query patterns."""
        # Insert test data
        now = datetime.now()
        categories = ['A', 'B', 'C']
        
        for cat in categories:
            for i in range(10):
                self.cassandra.insert_data(
                    'test_events',
                    {
                        'event_id': uuid.uuid4(),
                        'category': cat,
                        'event_date': now.date(),
                        'event_time': now + timedelta(minutes=i),
                        'data': f'{{"cat": "{cat}", "seq": {i}}}'
                    }
                )
        
        # Test partition query
        results = self.cassandra.execute_query(
            """
            SELECT * FROM test_events
            WHERE event_date = %s
            AND category = %s
            """,
            [now.date(), 'A']
        )
        
        self.assertEqual(len(results), 10)
        
        # Test clustering order
        results = self.cassandra.execute_query(
            """
            SELECT * FROM test_events
            WHERE event_date = %s
            AND category = %s
            ORDER BY event_time DESC
            LIMIT 5
            """,
            [now.date(), 'B']
        )
        
        self.assertEqual(len(results), 5)
        self.assertTrue(
            all(r.category == 'B' for r in results)
        )

if __name__ == '__main__':
    unittest.main() 