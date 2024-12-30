"""
Schema validation module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class SchemaValidator:
    """Data schema validation."""
    
    def __init__(self, config: Config):
        """Initialize validator."""
        self.config = config
        self.errors = []
    
    def validate(
        self,
        df: pd.DataFrame,
        schema_name: str
    ) -> bool:
        """
        Validate DataFrame against schema.
        
        Args:
            df: Input DataFrame
            schema_name: Schema name from config
        
        Returns:
            Whether validation passed
        """
        try:
            self.errors = []
            schema = self.config.schemas[schema_name]
            
            # Check required columns
            missing_cols = set(schema.keys()) - set(df.columns)
            if missing_cols:
                self.errors.append({
                    'type': 'missing_columns',
                    'columns': list(missing_cols)
                })
            
            # Validate each column
            for col, rules in schema.items():
                if col in df.columns:
                    self._validate_column(df[col], col, rules)
            
            valid = len(self.errors) == 0
            if not valid:
                logger.warning(
                    f"Schema validation failed: {len(self.errors)} errors"
                )
            
            return valid
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    def _validate_column(
        self,
        series: pd.Series,
        column: str,
        rules: Dict[str, Any]
    ) -> None:
        """Validate single column."""
        try:
            # Check data type
            if 'type' in rules:
                valid_type = self._check_type(series, rules['type'])
                if not valid_type:
                    self.errors.append({
                        'type': 'invalid_type',
                        'column': column,
                        'expected': rules['type']
                    })
            
            # Check required
            if rules.get('required', False):
                null_count = series.isnull().sum()
                if null_count > 0:
                    self.errors.append({
                        'type': 'null_values',
                        'column': column,
                        'count': null_count
                    })
            
            # Check unique
            if rules.get('unique', False):
                if not series.is_unique:
                    self.errors.append({
                        'type': 'duplicate_values',
                        'column': column
                    })
            
            # Check range
            if 'range' in rules:
                range_rules = rules['range']
                if 'min' in range_rules and series.min() < range_rules['min']:
                    self.errors.append({
                        'type': 'below_minimum',
                        'column': column,
                        'min': range_rules['min']
                    })
                if 'max' in range_rules and series.max() > range_rules['max']:
                    self.errors.append({
                        'type': 'above_maximum',
                        'column': column,
                        'max': range_rules['max']
                    })
            
            # Check values
            if 'values' in rules:
                invalid_values = ~series.isin(rules['values'])
                if invalid_values.any():
                    self.errors.append({
                        'type': 'invalid_values',
                        'column': column,
                        'count': invalid_values.sum()
                    })
                    
        except Exception as e:
            logger.error(f"Column validation failed: {e}")
            raise
    
    def _check_type(self, series: pd.Series, expected_type: str) -> bool:
        """Check column data type."""
        try:
            if expected_type == 'string':
                return series.dtype == 'object'
            elif expected_type == 'integer':
                return pd.to_numeric(series, errors='coerce').notna().all()
            elif expected_type == 'float':
                return pd.to_numeric(series, errors='coerce').notna().all()
            elif expected_type == 'date':
                return pd.to_datetime(series, errors='coerce').notna().all()
            elif expected_type == 'boolean':
                return series.isin([True, False, 0, 1]).all()
            else:
                raise ValueError(f"Unsupported type: {expected_type}")
        except Exception:
            return False
    
    def get_errors(self) -> List[Dict[str, Any]]:
        """Get validation errors."""
        return self.errors 