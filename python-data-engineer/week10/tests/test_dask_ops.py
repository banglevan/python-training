"""
Test DASK operations module.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, LocalCluster
import tempfile
import os
import shutil

from exercises.dask_ops import DaskOperations

class TestDaskOperations(unittest.TestCase):
    """Test DASK operations."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create temporary directory for test data
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test CSV files
        cls.test_data = pd.DataFrame({
            'id': range(100),
            'category': ['A', 'B'] * 50,
            'value': np.random.randn(100),
            'timestamp': pd.date_range('2024-01-01', periods=100)
        })
        
        cls.test_data.to_csv(
            f"{cls.temp_dir}/data_1.csv",
            index=False
        )
        cls.test_data.to_csv(
            f"{cls.temp_dir}/data_2.csv",
            index=False
        )
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
    
    def setUp(self):
        """Set up test fixtures."""
        self.dask_ops = DaskOperations(n_workers=2)
    
    def tearDown(self):
        """Clean up after each test."""
        self.dask_ops.cleanup()
    
    def test_load_data(self):
        """Test data loading."""
        # Load data
        df = self.dask_ops.load_data(
            self.temp_dir,
            "data_*.csv"
        )
        
        # Verify
        self.assertIsInstance(df, dd.DataFrame)
        result = df.compute()
        self.assertEqual(len(result), 200)  # Two files of 100 rows each
        self.assertTrue(all(col in result.columns 
                          for col in ['id', 'category', 'value', 'timestamp']))
    
    def test_process_dataframe(self):
        """Test DataFrame processing."""
        # Load data
        df = self.dask_ops.load_data(self.temp_dir)
        
        # Define operations
        operations = [
            {
                'type': 'filter',
                'column': 'category',
                'values': ['A']
            },
            {
                'type': 'transform',
                'source': 'value',
                'target': 'value_squared',
                'expression': 'x ** 2'
            },
            {
                'type': 'groupby',
                'columns': ['category'],
                'aggregations': {'value': 'mean'}
            }
        ]
        
        # Process
        result = self.dask_ops.process_dataframe(df, operations)
        computed = result.compute()
        
        # Verify
        self.assertIsInstance(result, dd.DataFrame)
        self.assertEqual(len(computed), 1)  # One category after groupby
        self.assertTrue('value_squared' in computed.columns)
    
    def test_matrix_operations(self):
        """Test matrix operations."""
        # Test different operations
        operations = ['transpose', 'inverse', 'svd']
        shape = (100, 100)
        chunk_size = (50, 50)
        
        for op in operations:
            result = self.dask_ops.matrix_operations(
                shape,
                chunk_size,
                op
            )
            
            self.assertIsInstance(result, da.Array)
            computed = result.compute()
            
            if op == 'transpose':
                self.assertEqual(computed.shape, (shape[1], shape[0]))
            elif op == 'inverse':
                self.assertEqual(computed.shape, shape)
            elif op == 'svd':
                self.assertEqual(computed.shape[0], shape[0])
    
    def test_time_series_analysis(self):
        """Test time series analysis."""
        # Load data
        df = self.dask_ops.load_data(self.temp_dir)
        
        # Analyze
        result = self.dask_ops.time_series_analysis(
            df,
            'timestamp',
            'value',
            freq='D'
        )
        
        # Verify
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(all(col in result.columns 
                          for col in ['mean', 'std', 'min', 'max', 'count']))
        self.assertTrue('rolling_avg' in result.columns)
    
    def test_parallel_apply(self):
        """Test parallel apply operation."""
        # Load data
        df = self.dask_ops.load_data(self.temp_dir)
        
        # Define function
        def square(x):
            return x ** 2
        
        # Apply
        result = self.dask_ops.parallel_apply(
            df,
            square,
            'value'
        )
        
        # Verify
        self.assertIsInstance(result, dd.Series)
        computed = result.compute()
        self.assertEqual(len(computed), 200)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test invalid file path
        with self.assertRaises(Exception):
            self.dask_ops.load_data("/invalid/path")
        
        # Test invalid operation
        df = self.dask_ops.load_data(self.temp_dir)
        with self.assertRaises(Exception):
            self.dask_ops.process_dataframe(df, [{
                'type': 'invalid_operation'
            }])
        
        # Test invalid matrix operation
        with self.assertRaises(ValueError):
            self.dask_ops.matrix_operations(
                (100, 100),
                (50, 50),
                'invalid_op'
            )
    
    def test_large_data_processing(self):
        """Test processing of larger datasets."""
        # Create larger test data
        large_df = pd.DataFrame({
            'id': range(10000),
            'category': ['A', 'B', 'C', 'D'] * 2500,
            'value': np.random.randn(10000),
            'timestamp': pd.date_range('2024-01-01', periods=10000)
        })
        
        large_df.to_csv(f"{self.temp_dir}/large_data.csv", index=False)
        
        # Load and process
        df = self.dask_ops.load_data(
            self.temp_dir,
            "large_data.csv"
        )
        
        operations = [
            {
                'type': 'filter',
                'column': 'category',
                'values': ['A', 'B']
            },
            {
                'type': 'transform',
                'source': 'value',
                'target': 'value_scaled',
                'expression': 'x * 100'
            },
            {
                'type': 'groupby',
                'columns': ['category'],
                'aggregations': {
                    'value': ['mean', 'std', 'count']
                }
            }
        ]
        
        result = self.dask_ops.process_dataframe(df, operations)
        computed = result.compute()
        
        # Verify
        self.assertIsInstance(computed, pd.DataFrame)
        self.assertEqual(len(computed), 2)  # Two categories after filtering
    
    def test_cluster_management(self):
        """Test cluster management."""
        # Verify cluster is running
        self.assertIsNotNone(self.dask_ops.client)
        self.assertIsNotNone(self.dask_ops.cluster)
        
        # Test cleanup
        self.dask_ops.cleanup()
        
        # Verify cluster is closed
        self.assertTrue(self.dask_ops.cluster.status == 'closed')

if __name__ == '__main__':
    unittest.main() 