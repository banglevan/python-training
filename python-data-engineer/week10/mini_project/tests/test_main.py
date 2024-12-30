"""
Test main ETL orchestration system.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime
import os
import tempfile
import json
from main import main

class TestMain(unittest.TestCase):
    """Test main orchestration system."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'DATABASE_URL': 'sqlite:///:memory:',
            'METRICS_PORT': '8000',
            'STATSD_HOST': 'localhost',
            'STATSD_PORT': '8125'
        })
        self.env_patcher.start()
        
        # Create test data
        self.test_data = pd.DataFrame({
            'id': range(100),
            'value': range(100),
            'category': ['A', 'B'] * 50
        })
    
    def tearDown(self):
        """Clean up test environment."""
        self.env_patcher.stop()
    
    @patch('main.Orchestrator')
    def test_main_success(self, mock_orchestrator):
        """Test successful pipeline execution."""
        # Setup mock
        mock_instance = Mock()
        mock_instance.run_pipeline.return_value = {
            'status': 'success',
            'records_processed': 100
        }
        mock_orchestrator.return_value = mock_instance
        
        # Execute
        with patch('sys.argv', ['main.py', '--engine', 'airflow']):
            main()
        
        # Verify
        mock_orchestrator.assert_called_once_with('airflow')
        mock_instance.run_pipeline.assert_called_once()
        call_args = mock_instance.run_pipeline.call_args[0]
        self.assertEqual(call_args[0], 'example_pipeline')
        self.assertTrue('source_system' in call_args[1])
        self.assertTrue('execution_date' in call_args[1])
    
    @patch('main.Orchestrator')
    def test_main_failure(self, mock_orchestrator):
        """Test pipeline failure handling."""
        # Setup mock to raise exception
        mock_instance = Mock()
        mock_instance.run_pipeline.side_effect = Exception("Pipeline failed")
        mock_orchestrator.return_value = mock_instance
        
        # Execute and verify exception is raised
        with patch('sys.argv', ['main.py']):
            with self.assertRaises(Exception):
                main()
    
    @patch('main.Orchestrator')
    def test_main_invalid_engine(self, mock_orchestrator):
        """Test invalid engine handling."""
        # Execute with invalid engine
        with patch('sys.argv', ['main.py', '--engine', 'invalid']):
            with self.assertRaises(SystemExit):
                main()
    
    @patch('main.Orchestrator')
    def test_main_context_structure(self, mock_orchestrator):
        """Test context structure passed to pipeline."""
        # Setup mock
        mock_instance = Mock()
        mock_instance.run_pipeline.return_value = {'status': 'success'}
        mock_orchestrator.return_value = mock_instance
        
        # Execute
        with patch('sys.argv', ['main.py']):
            main()
        
        # Verify context structure
        context = mock_instance.run_pipeline.call_args[0][1]
        self.assertIn('source_system', context)
        self.assertIn('execution_date', context)
        self.assertIsInstance(context['execution_date'], datetime)
    
    @patch('main.logger')
    @patch('main.Orchestrator')
    def test_main_logging(self, mock_orchestrator, mock_logger):
        """Test logging behavior."""
        # Setup mock
        mock_instance = Mock()
        mock_instance.run_pipeline.return_value = {'status': 'success'}
        mock_orchestrator.return_value = mock_instance
        
        # Execute
        with patch('sys.argv', ['main.py']):
            main()
        
        # Verify logging
        mock_logger.info.assert_called()
        self.assertTrue(
            any('completed successfully' in str(call) 
                for call in mock_logger.info.call_args_list)
        )

if __name__ == '__main__':
    unittest.main() 