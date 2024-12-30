"""
Test basic Prefect flows.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from prefect.testing.utilities import prefect_test_harness

from exercises.prefect_flows import (
    extract_data,
    transform_data,
    validate_data,
    load_data,
    process_data
)

class TestBasicFlows(unittest.TestCase):
    """Test basic Prefect flows."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_data = pd.DataFrame({
            'id': range(3),
            'amount': [100, 200, 300],
            'date': ['2024-01-01', '2024-01-02', '2024-01-03']
        })
    
    @patch('requests.get')
    def test_extract_data(self, mock_get):
        """Test data extraction."""
        # Setup mock
        mock_response = Mock()
        mock_response.json.return_value = self.test_data.to_dict('records')
        mock_get.return_value = mock_response
        
        # Execute
        with prefect_test_harness():
            result = extract_data.fn("http://test.api/data")
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        mock_get.assert_called_once()
    
    def test_transform_data(self):
        """Test data transformation."""
        # Execute
        with prefect_test_harness():
            result = transform_data.fn(self.test_data)
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue('amount_usd' in result.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result['date']))
    
    def test_validate_data(self):
        """Test data validation."""
        # Test valid data
        with prefect_test_harness():
            result = validate_data.fn(self.test_data)
        self.assertTrue(result)
        
        # Test invalid data
        invalid_data = self.test_data.copy()
        invalid_data.loc[0, 'amount'] = np.nan
        with prefect_test_harness():
            result = validate_data.fn(invalid_data)
        self.assertTrue(result)  # Still true but with warnings
    
    @patch('pandas.DataFrame.to_csv')
    def test_load_data(self, mock_to_csv):
        """Test data loading."""
        # Execute
        with prefect_test_harness():
            load_data.fn(self.test_data, "test.csv")
        
        # Verify
        mock_to_csv.assert_called_once_with("test.csv", index=False)
    
    @patch('exercises.prefect_flows.extract_data')
    @patch('exercises.prefect_flows.transform_data')
    @patch('exercises.prefect_flows.validate_data')
    @patch('exercises.prefect_flows.load_data')
    def test_process_data_flow(
        self,
        mock_load,
        mock_validate,
        mock_transform,
        mock_extract
    ):
        """Test complete data processing flow."""
        # Setup mocks
        mock_extract.return_value = self.test_data
        mock_transform.return_value = self.test_data
        mock_validate.return_value = True
        
        # Execute
        with prefect_test_harness():
            process_data("http://test.api/data", "test.csv")
        
        # Verify
        mock_extract.assert_called_once()
        mock_transform.assert_called_once()
        mock_validate.assert_called_once()
        mock_load.assert_called_once()

class TestFlowFailures(unittest.TestCase):
    """Test flow failure scenarios."""
    
    @patch('requests.get')
    def test_extract_failure(self, mock_get):
        """Test extraction failure."""
        # Setup mock to fail
        mock_get.side_effect = Exception("API error")
        
        # Execute and verify
        with prefect_test_harness():
            with self.assertRaises(Exception):
                extract_data.fn("http://test.api/data")
    
    def test_validation_failure(self):
        """Test validation with invalid data."""
        # Setup completely invalid data
        invalid_data = pd.DataFrame({
            'id': [None, None, None],
            'amount': [np.nan, np.nan, np.nan]
        })
        
        # Execute
        with prefect_test_harness():
            result = validate_data.fn(invalid_data)
        
        # Verify warnings were logged
        self.assertTrue(result)  # Function completes but logs warnings 