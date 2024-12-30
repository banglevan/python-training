"""
Test feature engineering operations.
"""

import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import os
import numpy as np
import pandas as pd
from datetime import datetime
from exercises.feature_engineering import (
    FeatureExtractor,
    FeatureStore,
    OnlineServing
)

class TestFeatureEngineering(unittest.TestCase):
    """Test feature engineering functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize test environment."""
        # Create test data
        cls.numeric_data = pd.DataFrame({
            'id': range(5),
            'value1': [1.0, 2.0, np.nan, 4.0, 5.0],
            'value2': [10.0, 20.0, 30.0, 40.0, 50.0]
        })
        
        cls.categorical_data = pd.DataFrame({
            'id': range(5),
            'category1': ['A', 'B', 'A', 'C', 'B'],
            'category2': ['X', 'Y', 'X', 'Z', 'Y']
        })
        
        cls.text_data = pd.DataFrame({
            'id': range(3),
            'text': [
                'This is sample text one',
                'This is sample text two',
                'This is different text'
            ]
        })
    
    def setUp(self):
        """Set up test case."""
        self.extractor = FeatureExtractor()
        self.feature_store = FeatureStore()
    
    def test_numeric_feature_extraction(self):
        """Test numeric feature extraction."""
        # Extract features
        result = self.extractor.extract_numeric_features(
            self.numeric_data,
            ['value1', 'value2'],
            scale=True
        )
        
        # Verify results
        self.assertIn('value1', result.columns)
        self.assertIn('value2', result.columns)
        self.assertFalse(result['value1'].isnull().any())
        
        # Verify scaling
        self.assertTrue(
            abs(result['value1'].mean()) < 1e-10
        )  # Close to 0
        self.assertTrue(
            abs(result['value1'].std() - 1.0) < 1e-10
        )  # Close to 1
    
    def test_categorical_feature_extraction(self):
        """Test categorical feature extraction."""
        # Extract features
        result = self.extractor.extract_categorical_features(
            self.categorical_data,
            ['category1']
        )
        
        # Verify one-hot encoding
        self.assertIn('category1_A', result.columns)
        self.assertIn('category1_B', result.columns)
        self.assertIn('category1_C', result.columns)
        self.assertNotIn('category1', result.columns)
        
        # Verify encoding values
        self.assertEqual(
            result['category1_A'].sum(),
            2  # Two 'A' values in original data
        )
    
    def test_text_feature_extraction(self):
        """Test text feature extraction."""
        # Extract features
        result = self.extractor.extract_text_features(
            self.text_data,
            ['text'],
            max_features=10
        )
        
        # Verify vectorization
        self.assertTrue(
            any('text_tfidf' in col for col in result.columns)
        )
        self.assertNotIn('text', result.columns)
        
        # Verify feature count
        text_features = [
            col for col in result.columns
            if col.startswith('text_tfidf')
        ]
        self.assertLessEqual(len(text_features), 10)
    
    @patch('redis.Redis')
    def test_feature_store(self, mock_redis):
        """Test feature store operations."""
        # Mock Redis client
        mock_client = Mock()
        mock_redis.return_value = mock_client
        
        store = FeatureStore()
        
        # Test feature storage
        features = {
            'numeric': 1.0,
            'categorical': 'A',
            'vector': [1.0, 2.0, 3.0]
        }
        
        store.store_features(features, 'entity1', ttl=3600)
        
        # Verify Redis calls
        mock_client.set.assert_called_once()
        mock_client.expire.assert_called_once()
        
        # Test feature retrieval
        mock_client.get.return_value = '{"numeric": 1.0}'
        result = store.get_features('entity1')
        
        self.assertIsNotNone(result)
        self.assertIn('numeric', result)
    
    def test_online_serving(self):
        """Test online serving."""
        # Create mock model
        class MockModel:
            def predict(self, X):
                return np.array([1.0])
        
        # Create temporary model file
        with tempfile.NamedTemporaryFile(suffix='.pkl') as tmp:
            import pickle
            pickle.dump(MockModel(), tmp)
            tmp.flush()
            
            # Initialize serving
            serving = OnlineServing(
                self.feature_store,
                tmp.name
            )
            
            # Test feature serving
            with patch.object(
                self.feature_store,
                'get_features',
                return_value={'feature1': 1.0}
            ):
                result = serving.serve_features('entity1')
                
                self.assertIsNotNone(result)
                self.assertIn('feature1', result)
                self.assertIn('prediction', result)

if __name__ == '__main__':
    unittest.main() 