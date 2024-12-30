"""
Unit tests for main application.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import yaml
from pathlib import Path
import tempfile
import os

from src.main import DataSyncApp
from src.monitoring import AlertSeverity

class TestDataSyncApp(unittest.TestCase):
    """Test cases for DataSyncApp."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary config file
        self.config = {
            'monitoring': {
                'metrics': {'database': {}},
                'alerts': {'default_severity': 'warning'}
            },
            'connectors': {
                'test_shopify': {
                    'type': 'shopify',
                    'name': 'test-store'
                }
            },
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topics': []
            },
            'flink': {
                'job_name': 'test-job'
            },
            'cache': {
                'host': 'localhost'
            },
            'sync': {
                'database': {}
            }
        }
        
        self.temp_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.temp_dir, 'test_config.yml')
        
        with open(self.config_path, 'w') as f:
            yaml.dump(self.config, f)
        
        # Create mocks
        self.mock_monitoring = Mock()
        self.mock_connector = Mock()
        self.mock_kafka = Mock()
        self.mock_flink = Mock()
        self.mock_cache = Mock()
        self.mock_sync = Mock()
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary config
        os.remove(self.config_path)
        os.rmdir(self.temp_dir)
    
    @patch('src.main.create_monitoring')
    @patch('src.main.create_connector')
    @patch('src.main.KafkaManager')
    @patch('src.main.FlinkProcessor')
    @patch('src.main.CacheManager')
    @patch('src.main.create_sync_manager')
    def test_initialize_success(
        self,
        mock_create_sync,
        mock_cache_cls,
        mock_flink_cls,
        mock_kafka_cls,
        mock_create_connector,
        mock_create_monitoring
    ):
        """Test successful initialization."""
        # Setup mocks
        mock_create_monitoring.return_value = self.mock_monitoring
        mock_create_connector.return_value = self.mock_connector
        mock_kafka_cls.return_value = self.mock_kafka
        mock_flink_cls.return_value = self.mock_flink
        mock_cache_cls.return_value = self.mock_cache
        mock_create_sync.return_value = self.mock_sync
        
        # Create app instance
        app = DataSyncApp(self.config_path)
        
        # Test initialization
        result = app.initialize()
        
        # Verify
        self.assertTrue(result)
        mock_create_monitoring.assert_called_once()
        mock_create_connector.assert_called_once()
        mock_kafka_cls.assert_called_once()
        mock_flink_cls.assert_called_once()
        mock_cache_cls.assert_called_once()
        mock_create_sync.assert_called_once()
        
        # Verify monitoring event
        self.mock_monitoring.record_event.assert_called_with(
            'connector',
            {
                'name': 'test_shopify',
                'type': 'shopify',
                'status': 'initialized'
            }
        )
    
    @patch('src.main.create_monitoring')
    def test_initialize_failure(self, mock_create_monitoring):
        """Test initialization failure."""
        # Setup mock to return None (failure)
        mock_create_monitoring.return_value = None
        
        # Create app instance
        app = DataSyncApp(self.config_path)
        
        # Test initialization
        with self.assertRaises(RuntimeError):
            app.initialize()
    
    @patch('src.main.create_monitoring')
    @patch('src.main.create_connector')
    @patch('src.main.KafkaManager')
    @patch('src.main.FlinkProcessor')
    @patch('src.main.CacheManager')
    @patch('src.main.create_sync_manager')
    def test_start_and_stop(
        self,
        mock_create_sync,
        mock_cache_cls,
        mock_flink_cls,
        mock_kafka_cls,
        mock_create_connector,
        mock_create_monitoring
    ):
        """Test application start and stop."""
        # Setup mocks
        mock_create_monitoring.return_value = self.mock_monitoring
        mock_create_connector.return_value = self.mock_connector
        mock_kafka_cls.return_value = self.mock_kafka
        mock_flink_cls.return_value = self.mock_flink
        mock_cache_cls.return_value = self.mock_cache
        mock_create_sync.return_value = self.mock_sync
        
        # Create and initialize app
        app = DataSyncApp(self.config_path)
        app.initialize()
        
        # Simulate KeyboardInterrupt during start
        self.mock_kafka.create_topics.side_effect = KeyboardInterrupt()
        
        # Test start and cleanup
        app.start()
        
        # Verify cleanup calls
        self.mock_connector.close.assert_called_once()
        self.mock_kafka.close.assert_called_once()
        self.mock_flink.stop.assert_called_once()
        self.mock_cache.close.assert_called_once()
        self.mock_sync.close.assert_called_once()
        self.mock_monitoring.close.assert_called_once()
    
    @patch('src.main.create_monitoring')
    @patch('src.main.create_connector')
    @patch('src.main.KafkaManager')
    @patch('src.main.FlinkProcessor')
    @patch('src.main.CacheManager')
    @patch('src.main.create_sync_manager')
    def test_health_check(
        self,
        mock_create_sync,
        mock_cache_cls,
        mock_flink_cls,
        mock_kafka_cls,
        mock_create_connector,
        mock_create_monitoring
    ):
        """Test health check functionality."""
        # Setup mocks
        mock_create_monitoring.return_value = self.mock_monitoring
        mock_create_connector.return_value = self.mock_connector
        mock_kafka_cls.return_value = self.mock_kafka
        mock_flink_cls.return_value = self.mock_flink
        mock_cache_cls.return_value = self.mock_cache
        mock_create_sync.return_value = self.mock_sync
        
        # Setup health check response
        self.mock_connector.health_check.return_value = {
            'connected': False,
            'error': 'Connection failed'
        }
        
        # Create and initialize app
        app = DataSyncApp(self.config_path)
        app.initialize()
        
        # Test health check
        app._check_health()
        
        # Verify monitoring calls
        self.mock_monitoring.record_event.assert_called_with(
            'health',
            {
                'name': 'test_shopify',
                'type': 'connector',
                'healthy': False,
                'details': {
                    'connected': False,
                    'error': 'Connection failed'
                }
            },
            alert=True,
            alert_severity=AlertSeverity.ERROR
        )
    
    def test_config_loading(self):
        """Test configuration loading."""
        # Test with invalid config path
        with self.assertRaises(Exception):
            DataSyncApp('invalid/path/config.yml')
        
        # Test with valid config
        app = DataSyncApp(self.config_path)
        self.assertEqual(app.config, self.config)

if __name__ == '__main__':
    unittest.main() 