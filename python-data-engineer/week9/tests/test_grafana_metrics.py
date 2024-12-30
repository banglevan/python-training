"""
Unit tests for Grafana metrics integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from requests.exceptions import RequestException

from exercises.grafana_metrics import GrafanaClient

class TestGrafanaClient(unittest.TestCase):
    """Test cases for GrafanaClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'base_url': 'http://localhost:3000',
            'api_key': 'test_key',
            'org_id': 1
        }
        
        self.client = GrafanaClient(self.config)
        self.mock_response = Mock()
        self.mock_response.status_code = 200
    
    def test_init(self):
        """Test client initialization."""
        self.assertEqual(self.client.base_url, 'http://localhost:3000')
        self.assertEqual(self.client.api_key, 'test_key')
        self.assertEqual(
            self.client.session.headers['Authorization'],
            'Bearer test_key'
        )
    
    @patch('requests.Session.post')
    def test_create_datasource(self, mock_post):
        """Test data source creation."""
        # Setup
        self.mock_response.json.return_value = {
            'datasource': {'id': 1}
        }
        mock_post.return_value = self.mock_response
        
        # Execute
        result = self.client.create_datasource(
            'test_ds',
            'postgres',
            'localhost:5432',
            'test_db',
            'test_user',
            'test_pass'
        )
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
        
        # Verify payload
        call_args = mock_post.call_args[1]['json']
        self.assertEqual(call_args['name'], 'test_ds')
        self.assertEqual(call_args['type'], 'postgres')
        self.assertEqual(call_args['database'], 'test_db')
    
    @patch('requests.Session.post')
    def test_create_dashboard(self, mock_post):
        """Test dashboard creation."""
        # Setup
        self.mock_response.json.return_value = {'uid': 'test123'}
        mock_post.return_value = self.mock_response
        
        panels = [{
            'title': 'Test Panel',
            'type': 'graph'
        }]
        
        # Execute
        result = self.client.create_dashboard(
            'Test Dashboard',
            panels,
            'now-1h',
            'now'
        )
        
        # Verify
        self.assertEqual(result, 'test123')
        mock_post.assert_called_once()
        
        # Verify payload
        call_args = mock_post.call_args[1]['json']
        self.assertEqual(
            call_args['dashboard']['title'],
            'Test Dashboard'
        )
        self.assertEqual(
            call_args['dashboard']['panels'],
            panels
        )
    
    @patch('requests.Session.post')
    def test_create_alert_rule(self, mock_post):
        """Test alert rule creation."""
        # Setup
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        query = {
            'refId': 'A',
            'datasourceUid': '1',
            'model': {'rawSql': 'SELECT 1'}
        }
        
        condition = {
            'evaluator': {
                'type': 'gt',
                'params': [5.0]
            }
        }
        
        # Execute
        result = self.client.create_alert_rule(
            'Test Alert',
            query,
            condition,
            1
        )
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_create_notification_channel(self, mock_post):
        """Test notification channel creation."""
        # Setup
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        settings = {
            'addresses': 'test@example.com'
        }
        
        # Execute
        result = self.client.create_notification_channel(
            'Test Channel',
            'email',
            settings
        )
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_error_handling(self, mock_post):
        """Test error handling."""
        # Setup
        mock_post.side_effect = RequestException('Connection error')
        
        # Execute and verify
        self.assertIsNone(
            self.client.create_datasource(
                'test',
                'postgres',
                'localhost',
                'test',
                'user',
                'pass'
            )
        )
        self.assertIsNone(
            self.client.create_dashboard('test', [])
        )
        self.assertIsNone(
            self.client.create_alert_rule('test', {}, {}, 1)
        )

if __name__ == '__main__':
    unittest.main() 