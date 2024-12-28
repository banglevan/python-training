"""Unit tests for Data Validation."""

import unittest
import pandas as pd
import numpy as np
from data_validation import DataValidator, ColumnValidation

class TestDataValidator(unittest.TestCase):
    """Test cases for Data Validator."""
    
    def setUp(self):
        """Set up test environment."""
        self.rules = [
            ColumnValidation(
                name="id",
                dtype="numeric",
                required=True,
                unique=True
            ),
            ColumnValidation(
                name="name",
                dtype="str",
                required=True,
                regex_pattern=r"^[A-Za-z\s]+$"
            ),
            ColumnValidation(
                name="age",
                dtype="numeric",
                min_value=0,
                max_value=120
            ),
            ColumnValidation(
                name="status",
                dtype="str",
                allowed_values=["active", "inactive"]
            )
        ]
        self.validator = DataValidator(self.rules)
    
    def test_validate_dtype(self):
        """Test data type validation."""
        df = pd.DataFrame({
            'numeric_col': ['1', '2', '3'],
            'invalid_col': ['a', 'b', '2']
        })
        
        self.assertTrue(
            self.validator.validate_dtype(
                df, 'numeric_col', 'numeric'
            )
        )
        self.assertFalse(
            self.validator.validate_dtype(
                df, 'invalid_col', 'numeric'
            )
        )
    
    def test_validate_missing(self):
        """Test missing value validation."""
        df = pd.DataFrame({
            'required_col': [1, None, 3],
            'optional_col': [1, None, 3]
        })
        
        self.assertFalse(
            self.validator.validate_missing(
                df, 'required_col', True
            )
        )
        self.assertTrue(
            self.validator.validate_missing(
                df, 'optional_col', False
            )
        )
    
    def test_validate_unique(self):
        """Test uniqueness validation."""
        df = pd.DataFrame({
            'unique_col': [1, 2, 3],
            'duplicate_col': [1, 1, 2]
        })
        
        self.assertTrue(
            self.validator.validate_unique(
                df, 'unique_col'
            )
        )
        self.assertFalse(
            self.validator.validate_unique(
                df, 'duplicate_col'
            )
        )
    
    def test_validate_range(self):
        """Test range validation."""
        df = pd.DataFrame({
            'value_col': [1, 5, 10]
        })
        
        self.assertTrue(
            self.validator.validate_range(
                df, 'value_col', 0, 15
            )
        )
        self.assertFalse(
            self.validator.validate_range(
                df, 'value_col', 2, 8
            )
        )
    
    def test_validate_allowed_values(self):
        """Test allowed values validation."""
        df = pd.DataFrame({
            'category': ['A', 'B', 'C']
        })
        
        self.assertTrue(
            self.validator.validate_allowed_values(
                df, 'category', ['A', 'B', 'C']
            )
        )
        self.assertFalse(
            self.validator.validate_allowed_values(
                df, 'category', ['A', 'B']
            )
        )
    
    def test_validate_regex(self):
        """Test regex pattern validation."""
        df = pd.DataFrame({
            'text': ['ABC', 'DEF', '123']
        })
        
        self.assertFalse(
            self.validator.validate_regex(
                df, 'text', r'^[A-Z]+$'
            )
        )
    
    def test_complete_validation(self):
        """Test complete validation process."""
        df = pd.DataFrame({
            'id': [1, 2, 2],  # Duplicate ID
            'name': ['John Doe', 'Jane123', 'Bob'],
            'age': [25, 150, -1],
            'status': ['active', 'pending', 'inactive']
        })
        
        results = self.validator.validate_dataframe(df)
        report = self.validator.get_validation_report()
        
        self.assertFalse(results['id'])
        self.assertFalse(results['name'])
        self.assertFalse(results['age'])
        self.assertFalse(results['status'])
        self.assertTrue(len(report['errors']) > 0)

if __name__ == '__main__':
    unittest.main() 