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
    """Schema validation management."""
    
    def __init__(self, config: Config):
        """Initialize validator."""
        self.config = config
        self.validation_errors = []
    
    def validate_schema(
        self,
        df: pd.DataFrame,
        dataset_name: str
    ) -> bool:
        """
        Validate dataset schema.
        
        Args:
            df: Input DataFrame
            dataset_name: Dataset name
        
        Returns:
            Whether validation passed
        """
        try:
            self.validation_errors = []
            schema = self.config.datasets[dataset_name]['schema']
            
            # Check required columns
            missing_cols = set(schema.keys()) - set(df.columns)
            if missing_cols:
                self.validation_errors.append({
                    'type': 'schema',
                    'error': f"Missing columns: {missing_cols}",
                    'dataset': dataset_name
                })
            
            # Validate data types
            for col, col_schema in schema.items():
                if col in df.columns:
                    if not self._validate_column_type(
                        df[col],
                        col_schema['type']
                    ):
                        self.validation_errors.append({
                            'type': 'data_type',
                            'error': f"Invalid type for column: {col}",
                            'expected': col_schema['type'],
                            'dataset': dataset_name
                        })
            
            valid = len(self.validation_errors) == 0
            if not valid:
                logger.warning(
                    f"Schema validation failed for {dataset_name}: "
                    f"{len(self.validation_errors)} errors"
                )
            
            return valid
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            raise
    
    def _validate_column_type(
        self,
        series: pd.Series,
        expected_type: str
    ) -> bool:
        """Validate column data type."""
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
    
    def get_validation_errors(self) -> List[Dict[str, Any]]:
        """Get validation errors."""
        return self.validation_errors 