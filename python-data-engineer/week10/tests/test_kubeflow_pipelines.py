"""
Test Kubeflow pipelines.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import kfp
from kfp import dsl
import pandas as pd
import numpy as np
import os
import tempfile
import shutil
import json

from exercises.kubeflow_pipelines import (
    load_data,
    preprocess_data,
    train_model,
    evaluate_model,
    ml_pipeline
)

class TestKubeflowComponents(unittest.TestCase):
    """Test Kubeflow pipeline components."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create temporary directory
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test data
        cls.test_data = pd.DataFrame({
            'feature1': np.random.randn(100),
            'feature2': np.random.randn(100),
            'target': np.random.randn(100)
        })
        
        # Save test data
        cls.source_path = os.path.join(cls.temp_dir, 'test_data.csv')
        cls.test_data.to_csv(cls.source_path, index=False)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
    
    def test_load_data(self):
        """Test data loading component."""
        # Create component
        load_op = load_data(self.source_path)
        
        # Execute
        output = load_op.python_func(self.source_path)
        
        # Verify
        self.assertTrue(os.path.exists(output.data_path))
        self.assertEqual(output.stats['rows'], 100)
        self.assertEqual(output.stats['columns'], 3)
    
    def test_preprocess_data(self):
        """Test data preprocessing component."""
        # Load data first
        load_output = load_data.python_func(self.source_path)
        
        # Create component
        preprocess_op = preprocess_data(
            load_output.data_path,
            'target',
            0.2
        )
        
        # Execute
        output = preprocess_op.python_func(
            load_output.data_path,
            'target',
            0.2
        )
        
        # Verify
        self.assertTrue(os.path.exists(output.train_path))
        self.assertTrue(os.path.exists(output.test_path))
        self.assertEqual(
            output.preprocessing_stats['features'],
            ['feature1', 'feature2']
        )
    
    def test_train_model(self):
        """Test model training component."""
        # Prepare data
        load_output = load_data.python_func(self.source_path)
        preprocess_output = preprocess_data.python_func(
            load_output.data_path,
            'target',
            0.2
        )
        
        # Create component
        model_params = {'n_estimators': 10}
        train_op = train_model(
            preprocess_output.train_path,
            'target',
            model_params
        )
        
        # Execute
        output = train_op.python_func(
            preprocess_output.train_path,
            'target',
            model_params
        )
        
        # Verify
        self.assertTrue(os.path.exists(output.model_path))
        self.assertTrue('feature_importance' in output.training_stats)
    
    def test_evaluate_model(self):
        """Test model evaluation component."""
        # Prepare data and model
        load_output = load_data.python_func(self.source_path)
        preprocess_output = preprocess_data.python_func(
            load_output.data_path,
            'target',
            0.2
        )
        train_output = train_model.python_func(
            preprocess_output.train_path,
            'target',
            {'n_estimators': 10}
        )
        
        # Create component
        evaluate_op = evaluate_model(
            train_output.model_path,
            preprocess_output.test_path,
            'target'
        )
        
        # Execute
        output = evaluate_op.python_func(
            train_output.model_path,
            preprocess_output.test_path,
            'target'
        )
        
        # Verify
        self.assertTrue(os.path.exists(output.metrics_path))
        self.assertTrue(all(metric in output.evaluation_stats 
                          for metric in ['mse', 'rmse', 'r2']))
    
    def test_pipeline_compilation(self):
        """Test pipeline compilation."""
        # Compile pipeline
        pipeline_path = os.path.join(self.temp_dir, 'pipeline.yaml')
        kfp.compiler.Compiler().compile(
            ml_pipeline,
            pipeline_path
        )
        
        # Verify
        self.assertTrue(os.path.exists(pipeline_path))
        
class TestKubeflowErrors(unittest.TestCase):
    """Test Kubeflow error handling."""
    
    def test_load_invalid_data(self):
        """Test handling of invalid data source."""
        with self.assertRaises(Exception):
            load_data.python_func('invalid/path.csv')
    
    def test_invalid_target(self):
        """Test handling of invalid target column."""
        with tempfile.NamedTemporaryFile(suffix='.csv') as f:
            pd.DataFrame({'x': [1, 2]}).to_csv(f.name, index=False)
            
            with self.assertRaises(Exception):
                preprocess_data.python_func(
                    f.name,
                    'invalid_target',
                    0.2
                )
    
    def test_invalid_model_params(self):
        """Test handling of invalid model parameters."""
        with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
            pd.DataFrame({
                'x': [1, 2],
                'target': [0, 1]
            }).to_parquet(f.name)
            
            with self.assertRaises(Exception):
                train_model.python_func(
                    f.name,
                    'target',
                    {'invalid_param': 0}
                )

if __name__ == '__main__':
    unittest.main() 