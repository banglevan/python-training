"""
Unit tests for Power BI integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import msal
import requests

from exercises.powerbi_integration import PowerBIClient

class TestPowerBIClient(unittest.TestCase):
    """Test cases for PowerBIClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'tenant_id': 'test_tenant',
            'client_id': 'test_client',
            'client_secret': 'test_secret',
            'workspace_id': 'test_workspace'
        }
        
        # Mock MSAL app
        self.mock_msal_app = Mock()
        self.mock_msal_app.acquire_token_for_client.return_value = {
            'access_token': 'test_token'
        }
        
        # Create test data
        self.test_data = pd.DataFrame({
            'metric_id': ['1', '2'],
            'value': [10.0, 20.0],
            'timestamp': pd.date_range('2024-01-01', periods=2)
        })
    
    @patch('msal.ConfidentialClientApplication')
    def test_init(self, mock_msal_cls):
        """Test client initialization."""
        # Setup
        mock_msal_cls.return_value = self.mock_msal_app
        
        # Execute
        client = PowerBIClient(self.config)
        
        # Verify
        mock_msal_cls.assert_called_once_with(
            client_id='test_client',
            client_credential='test_secret',
            authority='https://login.microsoftonline.com/test_tenant'
        )
        self.assertEqual(client.access_token, 'test_token')
    
    @patch('msal.ConfidentialClientApplication')
    @patch('requests.Session')
    def test_create_dataset(self, mock_session_cls, mock_msal_cls):
        """Test dataset creation."""
        # Setup
        mock_msal_cls.return_value = self.mock_msal_app
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {'id': 'ds1'}
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session
        
        client = PowerBIClient(self.config)
        
        tables = [{
            'name': 'TestTable',
            'columns': [
                {'name': 'id', 'dataType': 'string'},
                {'name': 'value', 'dataType': 'double'}
            ]
        }]
        
        # Execute
        result = client.create_dataset('Test Dataset', tables)
        
        # Verify
        self.assertEqual(result, 'ds1')
        mock_session.post.assert_called_once()
    
    @patch('msal.ConfidentialClientApplication')
    @patch('requests.Session')
    def test_push_data(self, mock_session_cls, mock_msal_cls):
        """Test data pushing."""
        # Setup
        mock_msal_cls.return_value = self.mock_msal_app
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session
        
        client = PowerBIClient(self.config)
        
        # Execute
        result = client.push_data(
            'ds1',
            'TestTable',
            self.test_data
        )
        
        # Verify
        self.assertTrue(result)
        mock_session.post.assert_called()
    
    @patch('msal.ConfidentialClientApplication')
    @patch('requests.Session')
    def test_create_report(self, mock_session_cls, mock_msal_cls):
        """Test report creation."""
        # Setup
        mock_msal_cls.return_value = self.mock_msal_app
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.return_value = {'id': 'report1'}
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session
        
        client = PowerBIClient(self.config)
        
        definition = {
            'version': '1.0',
            'pages': [{
                'name': 'Test Page',
                'visuals': []
            }]
        }
        
        # Execute
        result = client.create_report(
            'Test Report',
            'ds1',
            definition
        )
        
        # Verify
        self.assertEqual(result, 'report1')
        mock_session.post.assert_called_once()
    
    @patch('msal.ConfidentialClientApplication')
    @patch('requests.Session')
    def test_refresh_dataset(self, mock_session_cls, mock_msal_cls):
        """Test dataset refresh."""
        # Setup
        mock_msal_cls.return_value = self.mock_msal_app
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.post.return_value = mock_response
        mock_session_cls.return_value = mock_session
        
        client = PowerBIClient(self.config)
        
        # Execute
        result = client.refresh_dataset('ds1')
        
        # Verify
        self.assertTrue(result)
        mock_session.post.assert_called_once()
    
    @patch('msal.ConfidentialClientApplication')
    def test_token_error(self, mock_msal_cls):
        """Test token acquisition error."""
        # Setup
        mock_msal_cls.return_value = Mock(
            acquire_token_for_client=Mock(return_value={})
        )
        
        # Execute and verify
        with self.assertRaises(ValueError):
            PowerBIClient(self.config)

if __name__ == '__main__':
    unittest.main() 