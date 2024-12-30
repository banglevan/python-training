"""
Test Luigi tasks.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import luigi
import pandas as pd
import numpy as np
from datetime import datetime
import os
import tempfile
import shutil
import json

from exercises.luigi_tasks import (
    DataIngestionTask,
    DataValidationTask,
    DataTransformationTask,
    DataExportTask
)

class TestLuigiTasks(unittest.TestCase):
    """Test Luigi pipeline tasks."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create temporary directory
        cls.temp_dir = tempfile.mkdtemp()
        
        # Create test data
        cls.test_data = pd.DataFrame({
            'id': range(100),
            'value': np.random.uniform(0, 1000, 100),
            'category': ['A', 'B', 'C'] * 33 + ['A']
        })
        
        # Save test data
        cls.source_path = os.path.join(cls.temp_dir, 'source.csv')
        cls.test_data.to_csv(cls.source_path, index=False)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        shutil.rmtree(cls.temp_dir)
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_date = datetime.now()
    
    def test_data_ingestion(self):
        """Test data ingestion task."""
        task = DataIngestionTask(
            date=self.test_date,
            source_path=self.source_path
        )
        
        # Run task
        luigi.build([task], local_scheduler=True)
        
        # Verify
        self.assertTrue(task.output().exists())
        df = pd.read_parquet(task.output().path)
        self.assertEqual(len(df), 100)
        self.assertTrue(all(col in df.columns 
                          for col in ['id', 'value', 'category']))
    
    def test_data_validation(self):
        """Test data validation task."""
        validation_rules = {
            'id_check': {
                'type': 'unique_check',
                'column': 'id',
                'min_unique': 100
            },
            'value_check': {
                'type': 'range_check',
                'column': 'value',
                'min': 0,
                'max': 1000
            }
        }
        
        task = DataValidationTask(
            date=self.test_date,
            validation_rules=validation_rules
        )
        
        # Run task
        luigi.build([task], local_scheduler=True)
        
        # Verify
        self.assertTrue(task.output().exists())
        with task.output().open('r') as f:
            results = json.load(f)
        
        self.assertTrue(all(check['passed'] 
                          for check in results['checks'].values()))
    
    def test_data_transformation(self):
        """Test data transformation task."""
        transformations = [
            {
                'type': 'calculate',
                'target': 'value_scaled',
                'expression': 'value * 2'
            },
            {
                'type': 'filter',
                'condition': 'value > 500'
            },
            {
                'type': 'aggregate',
                'group_by': ['category'],
                'aggregations': {'value': ['mean', 'count']}
            }
        ]
        
        task = DataTransformationTask(
            date=self.test_date,
            transformations=transformations
        )
        
        # Run task
        luigi.build([task], local_scheduler=True)
        
        # Verify
        self.assertTrue(task.output().exists())
        df = pd.read_parquet(task.output().path)
        self.assertTrue('value_scaled' in df.columns)
    
    def test_data_export(self):
        """Test data export task."""
        formats = ['csv', 'json', 'excel']
        
        for fmt in formats:
            task = DataExportTask(
                date=self.test_date,
                export_format=fmt
            )
            
            # Run task
            luigi.build([task], local_scheduler=True)
            
            # Verify
            self.assertTrue(task.output().exists())
    
    def test_pipeline_integration(self):
        """Test full pipeline integration."""
        export_task = DataExportTask(
            date=self.test_date,
            export_format='csv'
        )
        
        # Run full pipeline
        luigi.build([export_task], local_scheduler=True)
        
        # Verify all intermediate files exist
        self.assertTrue(
            DataIngestionTask(date=self.test_date).output().exists()
        )
        self.assertTrue(
            DataValidationTask(date=self.test_date).output().exists()
        )
        self.assertTrue(
            DataTransformationTask(date=self.test_date).output().exists()
        )
        self.assertTrue(export_task.output().exists())

class TestLuigiErrors(unittest.TestCase):
    """Test Luigi error handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.test_date = datetime.now()
    
    def test_invalid_source(self):
        """Test handling of invalid source path."""
        task = DataIngestionTask(
            date=self.test_date,
            source_path='invalid/path.csv'
        )
        
        with self.assertRaises(Exception):
            luigi.build([task], local_scheduler=True)
    
    def test_invalid_validation_rules(self):
        """Test handling of invalid validation rules."""
        task = DataValidationTask(
            date=self.test_date,
            validation_rules={'invalid': {'type': 'unknown'}}
        )
        
        with self.assertRaises(Exception):
            luigi.build([task], local_scheduler=True)
    
    def test_invalid_transformation(self):
        """Test handling of invalid transformation."""
        task = DataTransformationTask(
            date=self.test_date,
            transformations=[{'type': 'invalid'}]
        )
        
        with self.assertRaises(Exception):
            luigi.build([task], local_scheduler=True)

if __name__ == '__main__':
    unittest.main() 