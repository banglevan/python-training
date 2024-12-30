"""
Test advanced Prefect features.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from prefect.testing.utilities import prefect_test_harness

from exercises.prefect_advanced import (
    fetch_batch,
    process_batch,
    aggregate_results,
    save_results,
    process_batches
)

class TestAdvancedFeatures(unittest.TestCase):
    """Test advanced Prefect features."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_batch = pd.DataFrame({
            'id': range(100),
            'value': range(0, 1000, 10)
        })
        
        self.test_results = [
            {
                'count': 100,
                'sum': 49500,
                'mean': 495,
                'min': 0,
                'max': 990
            },
            {
                'count': 100,
                'sum': 49500,
                'mean': 495,
                'min': 0,
                'max': 990
            }
        ]
    
    def test_fetch_batch(self):
        """Test batch data fetching."""
        # Execute
        with prefect_test_harness():
            result = fetch_batch.fn(0)
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 100)
        self.assertEqual(result['id'].min(), 0)
        self.assertEqual(result['id'].max(), 99)
    
    def test_process_batch(self):
        """Test batch processing."""
        # Execute
        with prefect_test_harness():
            result = process_batch.fn(self.test_batch)
        
        # Verify
        self.assertIsInstance(result, dict)
        self.assertEqual(result['count'], 100)
        self.assertEqual(result['sum'], 49500)
        self.assertEqual(result['mean'], 495)
        self.assertEqual(result['min'], 0)
        self.assertEqual(result['max'], 990)
    
    def test_aggregate_results(self):
        """Test results aggregation."""
        # Execute
        with prefect_test_harness():
            result = aggregate_results.fn(self.test_results)
        
        # Verify
        self.assertEqual(result['total_count'], 200)
        self.assertEqual(result['total_sum'], 99000)
        self.assertEqual(result['overall_mean'], 495)
        self.assertEqual(result['overall_min'], 0)
        self.assertEqual(result['overall_max'], 990)
    
    @patch('builtins.open', new_callable=MagicMock)
    def test_save_results(self, mock_open):
        """Test results saving."""
        # Execute
        with prefect_test_harness():
            save_results.fn(self.test_results[0], "test.json")
        
        # Verify
        mock_open.assert_called_once_with("test.json", 'w')
    
    @patch('exercises.prefect_advanced.slack_webhook.notify')
    @patch('exercises.prefect_advanced.create_markdown_artifact')
    def test_process_batches_flow(self, mock_artifact, mock_notify):
        """Test complete batch processing flow."""
        # Execute
        with prefect_test_harness():
            process_batches(2, "test.json")
        
        # Verify
        mock_notify.assert_called_once()
        self.assertEqual(mock_artifact.call_count, 3)  # 2 batches + summary

class TestCaching(unittest.TestCase):
    """Test caching functionality."""
    
    def test_fetch_batch_caching(self):
        """Test batch fetching cache."""
        # Execute twice with same input
        with prefect_test_harness():
            result1 = fetch_batch.fn(0)
            result2 = fetch_batch.fn(0)
        
        # Verify results are identical
        pd.testing.assert_frame_equal(result1, result2)
    
    def test_process_batch_caching(self):
        """Test batch processing cache."""
        # Execute twice with same input
        with prefect_test_harness():
            result1 = process_batch.fn(self.test_batch)
            result2 = process_batch.fn(self.test_batch)
        
        # Verify results are identical
        self.assertEqual(result1, result2)

class TestErrorHandling(unittest.TestCase):
    """Test error handling."""
    
    @patch('exercises.prefect_advanced.slack_webhook.notify')
    def test_flow_failure_notification(self, mock_notify):
        """Test failure notification."""
        # Setup to fail
        with patch('exercises.prefect_advanced.fetch_batch') as mock_fetch:
            mock_fetch.side_effect = Exception("Batch fetch failed")
            
            # Execute
            with prefect_test_harness():
                with self.assertRaises(Exception):
                    process_batches(1, "test.json")
            
            # Verify notification was sent
            mock_notify.assert_called_once()
            self.assertIn("failed", mock_notify.call_args[0][0])

if __name__ == '__main__':
    unittest.main() 