"""
Test custom Airflow operators.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import requests

from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowException

from exercises.custom_operators import (
    DataValidationOperator,
    APIHook,
    DataAvailabilitySensor
)

class TestDataValidationOperator(unittest.TestCase):
    """Test data validation operator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.operator = DataValidationOperator(
            task_id='test_validation',
            table='test_table',
            validation_queries={
                'null_check': "SELECT COUNT(*) = 0 FROM {table} WHERE id IS NULL",
                'row_count': "SELECT COUNT(*) > 0 FROM {table}"
            }
        )
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_successful_validation(self, mock_hook):
        """Test successful data validation."""
        # Setup mock
        mock_hook.return_value.get_records.return_value = [(True,)]
        
        # Execute
        self.operator.execute(context={})
        
        # Verify
        self.assertEqual(mock_hook.return_value.get_records.call_count, 2)
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_failed_validation(self, mock_hook):
        """Test failed data validation."""
        # Setup mock
        mock_hook.return_value.get_records.return_value = [(False,)]
        
        # Execute and verify
        with self.assertRaises(ValueError):
            self.operator.execute(context={})

class TestAPIHook(unittest.TestCase):
    """Test API hook."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.hook = APIHook(
            api_conn_id='test_api',
            retry_limit=2,
            retry_delay=0
        )
    
    @provide_session
    def test_connection_creation(self, session=None):
        """Test API connection creation."""
        conn = Connection(
            conn_id='test_api',
            conn_type='http',
            host='https://api.example.com',
            login='test_user',
            password='test_pass',
            extra='{"headers": {"Authorization": "Bearer test"}}'
        )
        session.add(conn)
        session.commit()
        
        session = self.hook.get_conn()
        self.assertEqual(
            session.headers['Authorization'],
            'Bearer test'
        )
    
    @patch('requests.Session.request')
    def test_successful_request(self, mock_request):
        """Test successful API request."""
        # Setup mock
        mock_response = Mock()
        mock_response.json.return_value = {'data': 'test'}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response
        
        # Execute
        result = self.hook.run('test/endpoint')
        
        # Verify
        self.assertEqual(result['data'], 'test')
        mock_request.assert_called_once()
    
    @patch('requests.Session.request')
    def test_retry_mechanism(self, mock_request):
        """Test API retry mechanism."""
        # Setup mock to fail twice then succeed
        mock_request.side_effect = [
            requests.exceptions.RequestException(),
            requests.exceptions.RequestException(),
            MagicMock(
                json=lambda: {'data': 'test'},
                raise_for_status=lambda: None
            )
        ]
        
        # Execute
        result = self.hook.run('test/endpoint')
        
        # Verify
        self.assertEqual(result['data'], 'test')
        self.assertEqual(mock_request.call_count, 3)

class TestDataAvailabilitySensor(unittest.TestCase):
    """Test data availability sensor."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sensor = DataAvailabilitySensor(
            task_id='test_sensor',
            table='test_table'
        )
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_data_available(self, mock_hook):
        """Test when data is available."""
        # Setup mock
        mock_hook.return_value.get_records.return_value = [(True,)]
        
        # Execute
        result = self.sensor.poke(context={})
        
        # Verify
        self.assertTrue(result)
        mock_hook.return_value.get_records.assert_called_once()
    
    @patch('airflow.providers.postgres.hooks.postgres.PostgresHook')
    def test_data_not_available(self, mock_hook):
        """Test when data is not available."""
        # Setup mock
        mock_hook.return_value.get_records.return_value = [(False,)]
        
        # Execute
        result = self.sensor.poke(context={})
        
        # Verify
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main() 