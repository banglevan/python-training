"""
Unit tests for Superset integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import yaml
import tempfile
import os
from requests.exceptions import RequestException

from exercises.superset_integration import SupersetClient

class TestSupersetClient(unittest.TestCase):
    """Test cases for SupersetClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'base_url': 'http://localhost:8088',
            'username': 'test_user',
            'password': 'test_pass'
        }
        
        self.client = SupersetClient(self.config)
        self.mock_response = Mock()
        self.mock_response.status_code = 200
    
    @patch('requests.Session.post')
    def test_login_success(self, mock_post):
        """Test successful login."""
        # Setup
        self.mock_response.json.return_value = {
            'access_token': 'test_token'
        }
        mock_post.return_value = self.mock_response
        
        # Execute
        result = self.client._login()
        
        # Verify
        self.assertTrue(result)
        self.assertEqual(self.client.access_token, 'test_token')
        mock_post.assert_called_once_with(
            'http://localhost:8088/api/v1/security/login',
            json={
                'username': 'test_user',
                'password': 'test_pass',
                'provider': 'db'
            }
        )
    
    @patch('requests.Session.post')
    def test_login_failure(self, mock_post):
        """Test login failure."""
        # Setup
        self.mock_response.status_code = 401
        mock_post.return_value = self.mock_response
        
        # Execute
        result = self.client._login()
        
        # Verify
        self.assertFalse(result)
    
    @patch('requests.Session.post')
    def test_create_database(self, mock_post):
        """Test database creation."""
        # Setup
        self.mock_response.status_code = 201
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        # Execute
        result = self.client.create_database(
            'test_db',
            'postgresql://test:test@localhost/test'
        )
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_create_dataset(self, mock_post):
        """Test dataset creation."""
        # Setup
        self.mock_response.status_code = 201
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        # Execute
        result = self.client.create_dataset(1, 'public', 'test_table')
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_create_chart(self, mock_post):
        """Test chart creation."""
        # Setup
        self.mock_response.status_code = 201
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        metrics = [{
            'aggregate': 'COUNT',
            'column': {'column_name': 'id'}
        }]
        
        # Execute
        result = self.client.create_chart(
            1,
            'Test Chart',
            'pie',
            metrics,
            ['category']
        )
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.post')
    def test_create_dashboard(self, mock_post):
        """Test dashboard creation."""
        # Setup
        self.mock_response.status_code = 201
        self.mock_response.json.return_value = {'id': 1}
        mock_post.return_value = self.mock_response
        
        position = {
            'CHART-1': {
                'type': 'CHART',
                'id': 1,
                'position': {'x': 0, 'y': 0}
            }
        }
        
        # Execute
        result = self.client.create_dashboard('Test Dashboard', position)
        
        # Verify
        self.assertEqual(result, 1)
        mock_post.assert_called_once()
    
    @patch('requests.Session.put')
    def test_add_charts_to_dashboard(self, mock_put):
        """Test adding charts to dashboard."""
        # Setup
        self.mock_response.status_code = 200
        mock_put.return_value = self.mock_response
        
        # Execute
        result = self.client.add_charts_to_dashboard(1, [1, 2])
        
        # Verify
        self.assertTrue(result)
        mock_put.assert_called_once()

if __name__ == '__main__':
    unittest.main() 