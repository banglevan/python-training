"""Unit tests for Data Quality Check."""

import unittest
import pandas as pd
import numpy as np
import os
from data_quality import DataQualityChecker

class TestDataQualityChecker(unittest.TestCase):
    """Test cases for Data Quality Checker."""
    
    def setUp(self):
        """Set up test environment."""
        # Create test data
        self.df = pd.DataFrame({
            'id': range(100),
            'name': [f'User{i}' for i in range(100)],
            'age': np.random.randint(18, 80, 100),
            'salary': np.random.normal(50000, 10000, 100),
            'department': np.random.choice(
                ['IT', 'HR', 'Sales'],
                100
            ),
            'join_date': pd.date_range(
                '2020-01-01',
                periods=100
            )
        })
        
        # Add some noise
        self.df.loc[
            np.random.choice(100, 5),
            'age'
        ] = np.nan
        self.df.loc[
            np.random.choice(100, 3),
            'salary'
        ] = -1
        
        self.checker = DataQualityChecker(self.df)
    
    def test_profile_numeric(self):
        """Test numeric column profiling."""
        profile = self.checker.profile_numeric('age')
        
        self.assertEqual(profile.name, 'age')
        self.assertEqual(profile.dtype, 'numeric')
        self.assertEqual(profile.count, 100)
        self.assertTrue(profile.missing_count > 0)
        self.assertIsNotNone(profile.min_value)
        self.assertIsNotNone(profile.max_value)
        self.assertIsNotNone(profile.mean_value)
    
    def test_profile_categorical(self):
        """Test categorical column profiling."""
        profile = self.checker.profile_categorical(
            'department'
        )
        
        self.assertEqual(profile.name, 'department')
        self.assertEqual(profile.dtype, 'categorical')
        self.assertEqual(profile.unique_count, 3)
        self.assertIsNotNone(profile.most_common)
        self.assertIsNotNone(profile.least_common)
    
    def test_profile_datetime(self):
        """Test datetime column profiling."""
        profile = self.checker.profile_datetime(
            'join_date'
        )
        
        self.assertEqual(profile.name, 'join_date')
        self.assertEqual(profile.dtype, 'datetime')
        self.assertEqual(profile.count, 100)
        self.assertIsNotNone(profile.min_value)
        self.assertIsNotNone(profile.max_value)
    
    def test_profile_dataframe(self):
        """Test complete dataframe profiling."""
        self.checker.profile_dataframe()
        
        self.assertTrue('age' in self.checker.profiles)
        self.assertTrue(
            'department' in self.checker.profiles
        )
        self.assertTrue(
            'join_date' in self.checker.profiles
        )
    
    def test_calculate_quality_scores(self):
        """Test quality score calculation."""
        self.checker.profile_dataframe()
        self.checker.calculate_quality_scores()
        
        for column in self.df.columns:
            scores = self.checker.quality_scores[column]
            self.assertTrue(0 <= scores['completeness'] <= 1)
            self.assertTrue(0 <= scores['uniqueness'] <= 1)
            self.assertTrue(0 <= scores['validity'] <= 1)
            self.assertTrue(0 <= scores['overall'] <= 1)
    
    def test_generate_report(self):
        """Test report generation."""
        report_path = 'test_report.json'
        
        self.checker.profile_dataframe()
        self.checker.calculate_quality_scores()
        self.checker.generate_report(report_path)
        
        self.assertTrue(os.path.exists(report_path))
        
        # Clean up
        os.remove(report_path)

if __name__ == '__main__':
    unittest.main() 