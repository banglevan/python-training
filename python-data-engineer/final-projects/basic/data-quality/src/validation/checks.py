"""
Data quality checks module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import re
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class QualityChecker:
    """Data quality checks management."""
    
    def __init__(self, config: Config):
        """Initialize checker."""
        self.config = config
        self.check_results = []
    
    def check_completeness(
        self,
        df: pd.DataFrame,
        columns: List[str],
        threshold: float
    ) -> bool:
        """
        Check data completeness.
        
        Args:
            df: Input DataFrame
            columns: Columns to check
            threshold: Minimum completeness ratio
        
        Returns:
            Whether check passed
        """
        try:
            for col in columns:
                if col in df.columns:
                    completeness = 1 - (df[col].isna().sum() / len(df))
                    if completeness < threshold:
                        self.check_results.append({
                            'type': 'completeness',
                            'column': col,
                            'score': completeness,
                            'threshold': threshold,
                            'status': 'failed'
                        })
                        return False
                    else:
                        self.check_results.append({
                            'type': 'completeness',
                            'column': col,
                            'score': completeness,
                            'threshold': threshold,
                            'status': 'passed'
                        })
            
            return True
            
        except Exception as e:
            logger.error(f"Completeness check failed: {e}")
            raise
    
    def check_uniqueness(
        self,
        df: pd.DataFrame,
        columns: List[str],
        threshold: float = 1.0
    ) -> bool:
        """
        Check value uniqueness.
        
        Args:
            df: Input DataFrame
            columns: Columns to check
            threshold: Minimum uniqueness ratio
        
        Returns:
            Whether check passed
        """
        try:
            for col in columns:
                if col in df.columns:
                    uniqueness = 1 - (df[col].duplicated().sum() / len(df))
                    if uniqueness < threshold:
                        self.check_results.append({
                            'type': 'uniqueness',
                            'column': col,
                            'score': uniqueness,
                            'threshold': threshold,
                            'status': 'failed'
                        })
                        return False
                    else:
                        self.check_results.append({
                            'type': 'uniqueness',
                            'column': col,
                            'score': uniqueness,
                            'threshold': threshold,
                            'status': 'passed'
                        })
            
            return True
            
        except Exception as e:
            logger.error(f"Uniqueness check failed: {e}")
            raise
    
    def check_format(
        self,
        df: pd.DataFrame,
        column: str,
        pattern: str
    ) -> bool:
        """
        Check value format.
        
        Args:
            df: Input DataFrame
            column: Column to check
            pattern: Regex pattern
        
        Returns:
            Whether check passed
        """
        try:
            if column in df.columns:
                # Compile regex
                regex = re.compile(pattern)
                
                # Check matches
                matches = df[column].apply(
                    lambda x: bool(regex.match(str(x)))
                )
                
                score = matches.mean()
                if score < 1.0:
                    self.check_results.append({
                        'type': 'format',
                        'column': column,
                        'score': score,
                        'pattern': pattern,
                        'status': 'failed'
                    })
                    return False
                else:
                    self.check_results.append({
                        'type': 'format',
                        'column': column,
                        'score': score,
                        'pattern': pattern,
                        'status': 'passed'
                    })
            
            return True
            
        except Exception as e:
            logger.error(f"Format check failed: {e}")
            raise
    
    def check_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None
    ) -> bool:
        """
        Check value range.
        
        Args:
            df: Input DataFrame
            column: Column to check
            min_value: Minimum value
            max_value: Maximum value
        
        Returns:
            Whether check passed
        """
        try:
            if column in df.columns:
                values = pd.to_numeric(df[column], errors='coerce')
                in_range = True
                
                if min_value is not None:
                    in_range &= (values >= min_value).all()
                
                if max_value is not None:
                    in_range &= (values <= max_value).all()
                
                score = values.between(
                    min_value or -np.inf,
                    max_value or np.inf
                ).mean()
                
                if not in_range:
                    self.check_results.append({
                        'type': 'range',
                        'column': column,
                        'score': score,
                        'min_value': min_value,
                        'max_value': max_value,
                        'status': 'failed'
                    })
                    return False
                else:
                    self.check_results.append({
                        'type': 'range',
                        'column': column,
                        'score': score,
                        'min_value': min_value,
                        'max_value': max_value,
                        'status': 'passed'
                    })
            
            return True
            
        except Exception as e:
            logger.error(f"Range check failed: {e}")
            raise
    
    def get_check_results(self) -> List[Dict[str, Any]]:
        """Get check results."""
        return self.check_results 