"""Unit tests for ETL Pipeline."""

import unittest
import pandas as pd
import sqlite3
import os
from etl_basic import ETLPipeline

class TestETLPipeline(unittest.TestCase):
    """Test cases for ETL Pipeline."""
    
    def setUp(self):
        """Set up test environment."""
        self.test_db = "test_etl.db"
        self.pipeline = ETLPipeline(self.test_db)
        
        # Create test CSV
        self.test_csv = "test_data.csv"
        pd.DataFrame({
            'numeric_col': [1, 2, None],
            'string_col': ['a', None, 'c'],
            'date_col': ['2024-01-01', '2024-01-02', '2024-01-03'],
            'old_name': ['x', 'y', 'z']
        }).to_csv(self.test_csv, index=False)
        
        # Create test JSON
        self.test_json = "test_data.json"
        pd.DataFrame({
            'numeric_col': [4, 5, 6],
            'string_col': ['d', 'e', 'f'],
            'date_col': ['2024-01-04', '2024-01-05', '2024-01-06'],
            'old_name': ['w', 'v', 'u']
        }).to_json(self.test_json, orient='records')
    
    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)
        if os.path.exists(self.test_json):
            os.remove(self.test_json)
    
    def test_extract_csv(self):
        """Test CSV extraction."""
        df = self.pipeline.extract_csv(self.test_csv)
        self.assertEqual(len(df), 3)
        self.assertTrue('numeric_col' in df.columns)
    
    def test_extract_json(self):
        """Test JSON extraction."""
        data = self.pipeline.extract_json(self.test_json)
        self.assertEqual(len(data), 3)
        self.assertTrue('numeric_col' in data[0])
    
    def test_transform_data(self):
        """Test data transformation."""
        df = self.pipeline.extract_csv(self.test_csv)
        transformed = self.pipeline.transform_data(df)
        
        # Check missing value handling
        self.assertFalse(transformed['numeric_col'].isnull().any())
        self.assertFalse(transformed['string_col'].isnull().any())
        
        # Check date conversion
        self.assertTrue(
            pd.api.types.is_datetime64_dtype(
                transformed['date_col']
            )
        )
        
        # Check column renaming
        self.assertTrue('new_name' in transformed.columns)
        self.assertFalse('old_name' in transformed.columns)
    
    def test_load_data(self):
        """Test data loading."""
        df = pd.DataFrame({
            'test_col': [1, 2, 3]
        })
        self.pipeline.load_data(df, 'test_table')
        
        # Verify data in database
        with sqlite3.connect(self.test_db) as conn:
            loaded_df = pd.read_sql(
                'SELECT * FROM test_table',
                conn
            )
            self.assertEqual(len(loaded_df), 3)
            self.assertTrue('test_col' in loaded_df.columns)
    
    def test_complete_pipeline(self):
        """Test complete pipeline execution."""
        self.pipeline.run_pipeline(
            self.test_csv,
            'csv_table'
        )
        
        with sqlite3.connect(self.test_db) as conn:
            df = pd.read_sql(
                'SELECT * FROM csv_table',
                conn
            )
            self.assertEqual(len(df), 3)
            self.assertTrue('new_name' in df.columns)
            self.assertFalse(df.isnull().any().any())

if __name__ == '__main__':
    unittest.main() 