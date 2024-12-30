"""
Unit tests for Tableau integration.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import tableauserverclient as TSC
from pathlib import Path
import tempfile

from exercises.tableau_connection import TableauClient

class TestTableauClient(unittest.TestCase):
    """Test cases for TableauClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.config = {
            'server_url': 'https://test-server',
            'username': 'test_user',
            'password': 'test_pass',
            'site_id': 'test_site',
            'project_name': 'Test Project'
        }
        
        # Mock TSC components
        self.mock_server = Mock()
        self.mock_auth = Mock()
        self.mock_server.auth = self.mock_auth
        
        # Create test data
        self.test_data = pd.DataFrame({
            'metric_id': ['1', '2'],
            'value': [10.0, 20.0],
            'timestamp': pd.date_range('2024-01-01', periods=2)
        })
    
    @patch('tableauserverclient.Server')
    @patch('tableauserverclient.TableauAuth')
    def test_init(self, mock_auth_cls, mock_server_cls):
        """Test client initialization."""
        # Setup
        mock_server_cls.return_value = self.mock_server
        
        # Execute
        client = TableauClient(self.config)
        
        # Verify
        mock_auth_cls.assert_called_once_with(
            username='test_user',
            password='test_pass',
            site_id='test_site'
        )
        mock_server_cls.assert_called_once_with('https://test-server')
        self.mock_auth.sign_in.assert_called_once()
    
    @patch('tableauserverclient.Server')
    @patch('tableauserverclient.TableauAuth')
    def test_publish_datasource(self, mock_auth_cls, mock_server_cls):
        """Test datasource publication."""
        # Setup
        mock_server_cls.return_value = self.mock_server
        mock_project = Mock(id='proj1')
        mock_datasource = Mock(id='ds1')
        
        self.mock_server.projects.get.return_value = ([mock_project], None)
        self.mock_server.datasources.publish.return_value = mock_datasource
        
        client = TableauClient(self.config)
        
        # Execute
        result = client.publish_datasource(
            'Test DS',
            self.test_data,
            extract=False
        )
        
        # Verify
        self.assertEqual(result, 'ds1')
        self.mock_server.datasources.publish.assert_called_once()
    
    @patch('tableauserverclient.Server')
    @patch('tableauserverclient.TableauAuth')
    def test_create_workbook(self, mock_auth_cls, mock_server_cls):
        """Test workbook creation."""
        # Setup
        mock_server_cls.return_value = self.mock_server
        mock_project = Mock(id='proj1')
        mock_workbook = Mock(id='wb1')
        
        self.mock_server.projects.get.return_value = ([mock_project], None)
        self.mock_server.workbooks.publish.return_value = mock_workbook
        
        client = TableauClient(self.config)
        
        views = [{
            'name': 'Test View',
            'type': 'text',
            'columns': ['metric_id', 'value']
        }]
        
        # Execute
        result = client.create_workbook(
            'Test WB',
            'ds1',
            views
        )
        
        # Verify
        self.assertEqual(result, 'wb1')
        self.mock_server.workbooks.publish.assert_called_once()
    
    @patch('tableauserverclient.Server')
    @patch('tableauserverclient.TableauAuth')
    def test_get_project(self, mock_auth_cls, mock_server_cls):
        """Test project retrieval."""
        # Setup
        mock_server_cls.return_value = self.mock_server
        mock_project = Mock(name='Test Project', id='proj1')
        
        self.mock_server.projects.get.return_value = ([mock_project], None)
        
        client = TableauClient(self.config)
        
        # Execute
        result = client._get_project('Test Project')
        
        # Verify
        self.assertEqual(result.id, 'proj1')
        self.mock_server.projects.get.assert_called_once()
    
    @patch('tableauserverclient.Server')
    @patch('tableauserverclient.TableauAuth')
    def test_close(self, mock_auth_cls, mock_server_cls):
        """Test client cleanup."""
        # Setup
        mock_server_cls.return_value = self.mock_server
        client = TableauClient(self.config)
        
        # Execute
        client.close()
        
        # Verify
        self.mock_auth.sign_out.assert_called_once()

if __name__ == '__main__':
    unittest.main() 