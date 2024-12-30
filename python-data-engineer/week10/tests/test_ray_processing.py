"""
Test Ray processing module.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import ray
import pandas as pd
from datetime import datetime
import tempfile
import os

from exercises.ray_processing import (
    DataProcessor,
    ModelTrainer,
    RayProcessor
)

class TestDataProcessor(unittest.TestCase):
    """Test DataProcessor actor."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Ray for testing."""
        ray.init(num_cpus=2)
    
    @classmethod
    def tearDownClass(cls):
        """Shutdown Ray."""
        ray.shutdown()
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = DataProcessor.remote(
            processor_id=1,
            cache_size=100
        )
        self.test_data = np.random.randn(100, 10)
    
    def test_process_batch_normalize(self):
        """Test batch normalization."""
        # Execute
        result, metrics = ray.get(
            self.processor.process_batch.remote(
                self.test_data,
                'normalize'
            )
        )
        
        # Verify
        self.assertEqual(result.shape, self.test_data.shape)
        self.assertAlmostEqual(result.mean(), 0, places=7)
        self.assertAlmostEqual(result.std(), 1, places=7)
        self.assertTrue(metrics['success'])
    
    def test_process_batch_invalid_operation(self):
        """Test invalid operation handling."""
        with self.assertRaises(ValueError):
            ray.get(
                self.processor.process_batch.remote(
                    self.test_data,
                    'invalid_op'
                )
            )
    
    def test_process_batch_empty_data(self):
        """Test empty data handling."""
        with self.assertRaises(ValueError):
            ray.get(
                self.processor.process_batch.remote(
                    np.array([]),
                    'normalize'
                )
            )
    
    def test_moving_average(self):
        """Test moving average operation."""
        data = np.array([1, 2, 3, 4, 5])
        result, metrics = ray.get(
            self.processor.process_batch.remote(
                data,
                'moving_average',
                {'window': 3}
            )
        )
        
        self.assertEqual(len(result), len(data) - 2)  # Window size effect
        self.assertTrue(metrics['success'])
    
    def test_outlier_removal(self):
        """Test outlier removal."""
        data = np.array([1, 2, 100, 3, 4, 5])
        result, metrics = ray.get(
            self.processor.process_batch.remote(
                data,
                'outlier_removal',
                {'threshold': 2}
            )
        )
        
        self.assertTrue(len(result) < len(data))  # Outlier removed
        self.assertTrue(metrics['success'])
    
    def test_cache_management(self):
        """Test cache size management."""
        # Fill cache
        for i in range(150):  # More than cache_size
            ray.get(
                self.processor.process_batch.remote(
                    np.array([i]),
                    'normalize'
                )
            )
        
        # Check cache size
        stats = ray.get(self.processor.get_stats.remote())
        cache_size = int(stats['cache_usage'].split('/')[0])
        self.assertLessEqual(cache_size, 100)  # Max cache size
    
    def test_get_stats(self):
        """Test statistics retrieval."""
        # Process some data
        ray.get(
            self.processor.process_batch.remote(
                self.test_data,
                'normalize'
            )
        )
        
        # Get stats
        stats = ray.get(self.processor.get_stats.remote())
        
        # Verify
        self.assertEqual(stats['processor_id'], 1)
        self.assertEqual(stats['metrics']['processed_batches'], 1)
        self.assertTrue('average_batch_time' in stats['metrics'])
        self.assertTrue('items_per_second' in stats['metrics'])

class TestModelTrainer(unittest.TestCase):
    """Test ModelTrainer actor."""
    
    @classmethod
    def setUpClass(cls):
        """Initialize Ray for testing."""
        ray.init(num_cpus=2)
    
    @classmethod
    def tearDownClass(cls):
        """Shutdown Ray."""
        ray.shutdown()
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.trainer = ModelTrainer.remote(model_dir=self.temp_dir)
        
        # Generate test data
        np.random.seed(42)
        self.X = np.random.randn(1000, 10)
        self.y = np.random.randn(1000)
    
    def tearDown(self):
        """Clean up test files."""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_train_linear_model(self):
        """Test linear model training."""
        config = {
            'type': 'linear',
            'params': {}
        }
        
        result = ray.get(
            self.trainer.train.remote(
                self.X,
                self.y,
                config
            )
        )
        
        self.assertTrue('metrics' in result)
        self.assertTrue('model_path' in result)
        self.assertTrue(os.path.exists(result['model_path']))
    
    def test_train_with_invalid_config(self):
        """Test training with invalid configuration."""
        config = {
            'type': 'invalid_model',
            'params': {}
        }
        
        with self.assertRaises(ValueError):
            ray.get(
                self.trainer.train.remote(
                    self.X,
                    self.y,
                    config
                )
            )

class TestRayProcessor(unittest.TestCase):
    """Test RayProcessor manager."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.processor = RayProcessor(num_processors=2)
        self.test_data = np.random.randn(1000, 10)
    
    def tearDown(self):
        """Clean up."""
        self.processor.cleanup()
    
    def test_parallel_processing(self):
        """Test parallel data processing."""
        results, metrics = self.processor.parallel_processing(
            data=self.test_data,
            operation='normalize',
            batch_size=2
        )
        
        self.assertEqual(len(results), 2)  # Number of batches
        self.assertTrue('total_time' in metrics)
        self.assertTrue('items_per_second' in metrics)
    
    def test_distributed_training(self):
        """Test distributed model training."""
        X = np.random.randn(1000, 10)
        y = np.random.randn(1000)
        
        data = [{'X': X, 'y': y}]
        models = [{'type': 'linear', 'params': {}}]
        
        results = self.processor.distributed_training(
            data=data,
            models=models
        )
        
        self.assertTrue('best_model' in results)
        self.assertTrue('training_time' in results)
    
    def test_parameter_search(self):
        """Test parameter search functionality."""
        X = np.random.randn(1000, 10)
        y = np.random.randn(1000)
        
        param_grids = {
            'rf': {
                'n_estimators': [10, 20],
                'max_depth': [3, 5]
            }
        }
        
        results = self.processor.distributed_training(
            data=[{'X': X, 'y': y}],
            models=[],
            param_grids=param_grids
        )
        
        self.assertTrue('best_model' in results)
        self.assertTrue(len(results['all_results']) > 1)
    
    def test_processor_stats(self):
        """Test statistics collection."""
        # Process some data first
        self.processor.parallel_processing(
            data=self.test_data,
            operation='normalize',
            batch_size=2
        )
        
        stats = self.processor.get_processor_stats()
        
        self.assertTrue('aggregate_metrics' in stats)
        self.assertTrue('processor_stats' in stats)
        self.assertTrue(stats['aggregate_metrics']['total_processed_batches'] > 0)

if __name__ == '__main__':
    unittest.main() 