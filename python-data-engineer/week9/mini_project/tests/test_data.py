"""
Data management tests.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime

from src.data.sources import (
    PostgresSource,
    MongoSource,
    KafkaSource,
    SourceFactory
)
from src.data.processors import (
    TimeSeriesProcessor,
    AggregationProcessor,
    FilterProcessor,
    ProcessorPipeline
)
from src.data.cache import DataCache

class TestDataSources(unittest.TestCase):
    """Test data sources."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.pg_config = {
            'type': 'postgres',
            'name': 'test_source',
            'table': 'test_table'
        }
        self.test_data = pd.DataFrame({
            'id': range(3),
            'value': [10, 20, 30],
            'timestamp': pd.date_range('2024-01-01', periods=3)
        })
    
    @patch('src.core.database.DatabaseManager.get_pg_conn')
    def test_postgres_source(self, mock_conn):
        """Test PostgreSQL source."""
        # Setup
        mock_conn.return_value.__enter__.return_value = Mock()
        pd.read_sql = Mock(return_value=self.test_data)
        
        source = PostgresSource(self.pg_config)
        
        # Execute
        result = source.fetch_data({
            'filters': {'id': 1}
        })
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        pd.read_sql.assert_called_once()
    
    def test_source_factory(self):
        """Test source factory."""
        # Execute and verify
        source = SourceFactory.create_source(self.pg_config)
        self.assertIsInstance(source, PostgresSource)
        
        with self.assertRaises(ValueError):
            SourceFactory.create_source({'type': 'invalid'})

class TestDataProcessors(unittest.TestCase):
    """Test data processors."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=5, freq='H'),
            'value': [10, 20, 30, 40, 50],
            'category': ['A', 'A', 'B', 'B', 'C']
        })
    
    def test_time_series_processor(self):
        """Test time series processing."""
        # Setup
        config = {
            'time_column': 'timestamp',
            'value_column': 'value',
            'aggregation': 'mean',
            'resample_rule': '2H'
        }
        processor = TimeSeriesProcessor(config)
        
        # Execute
        result = processor.process(self.test_data)
        
        # Verify
        self.assertEqual(len(result), 3)  # 5 hours -> 3 2-hour periods
    
    def test_aggregation_processor(self):
        """Test data aggregation."""
        # Setup
        config = {
            'group_by': ['category'],
            'metrics': {'value': 'mean'}
        }
        processor = AggregationProcessor(config)
        
        # Execute
        result = processor.process(self.test_data)
        
        # Verify
        self.assertEqual(len(result), 3)  # 3 unique categories
    
    def test_processor_pipeline(self):
        """Test processing pipeline."""
        # Setup
        processors = [
            FilterProcessor({
                'conditions': [{
                    'column': 'value',
                    'operator': 'gt',
                    'value': 20
                }]
            }),
            AggregationProcessor({
                'group_by': ['category'],
                'metrics': {'value': 'mean'}
            })
        ]
        pipeline = ProcessorPipeline(processors)
        
        # Execute
        result = pipeline.process(self.test_data)
        
        # Verify
        self.assertTrue(result['value'].min() > 20)

class TestDataCache(unittest.TestCase):
    """Test data caching."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.cache = DataCache()
        self.test_data = pd.DataFrame({
            'id': range(3),
            'value': [10, 20, 30]
        })
    
    @patch('redis.Redis')
    def test_cache_operations(self, mock_redis):
        """Test cache operations."""
        # Setup
        mock_redis.return_value = Mock()
        
        # Execute
        source = "test_source"
        query = {"filters": {"id": 1}}
        
        # Test set
        success = self.cache.set(source, query, self.test_data)
        self.assertTrue(success)
        
        # Test get
        mock_redis.return_value.get.return_value = self.test_data.to_json()
        result = self.cache.get(source, query)
        self.assertIsInstance(result, pd.DataFrame)
        
        # Test invalidate
        success = self.cache.invalidate(source, query)
        self.assertTrue(success)

if __name__ == '__main__':
    unittest.main() 