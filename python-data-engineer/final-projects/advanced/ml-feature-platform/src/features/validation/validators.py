"""
Feature validation rules and checks.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from src.monitoring.metrics import MetricsCollector

class FeatureValidator:
    """Validate feature values and distributions."""
    
    def __init__(self):
        """Initialize validator."""
        self.metrics = MetricsCollector()
        
        # Define validation rules
        self.rules = {
            'missing_threshold': 0.1,  # Max 10% missing values
            'numeric_bounds': {
                'total_orders': (0, None),
                'total_amount': (0, None),
                'avg_order_value': (0, None),
                'days_since_last_order': (0, None),
                'order_frequency': (0, None),
                'return_rate': (0, 1),
                'review_score': (1, 5)
            }
        }
    
    def validate_features(
        self,
        features_df: pd.DataFrame,
        feature_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Validate features against defined rules.
        
        Args:
            features_df: DataFrame with features
            feature_names: Optional list of features to validate
            
        Returns:
            Dictionary with validation results
        """
        try:
            self.metrics.start_operation("validate_features")
            
            if feature_names is None:
                feature_names = features_df.columns
            
            results = {
                'missing_checks': self._check_missing_values(
                    features_df[feature_names]
                ),
                'bound_checks': self._check_numeric_bounds(
                    features_df[feature_names]
                ),
                'correlation_checks': self._check_correlations(
                    features_df[feature_names]
                ),
                'distribution_checks': self._check_distributions(
                    features_df[feature_names]
                )
            }
            
            duration = self.metrics.end_operation("validate_features")
            
            return results
            
        except Exception as e:
            self.metrics.record_error("validate_features")
            raise
    
    def _check_missing_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for missing values in features."""
        missing_rates = df.isnull().mean()
        
        failed_features = missing_rates[
            missing_rates > self.rules['missing_threshold']
        ]
        
        return {
            'failed_features': failed_features.to_dict(),
            'max_missing_rate': missing_rates.max(),
            'passed': len(failed_features) == 0
        }
    
    def _check_numeric_bounds(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check numeric features are within bounds."""
        failed_features = {}
        
        for feature, (min_val, max_val) in self.rules['numeric_bounds'].items():
            if feature not in df.columns:
                continue
                
            values = df[feature]
            
            if min_val is not None and values.min() < min_val:
                failed_features[feature] = {
                    'min_found': values.min(),
                    'min_allowed': min_val
                }
            
            if max_val is not None and values.max() > max_val:
                failed_features[feature] = {
                    'max_found': values.max(),
                    'max_allowed': max_val
                }
        
        return {
            'failed_features': failed_features,
            'passed': len(failed_features) == 0
        }
    
    def _check_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for high correlations between features."""
        numeric_df = df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return {
                'high_correlations': {},
                'passed': True
            }
        
        corr_matrix = numeric_df.corr()
        high_corr = {}
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) > 0.95:  # Threshold for high correlation
                    high_corr[f"{corr_matrix.columns[i]}_{corr_matrix.columns[j]}"] = corr
        
        return {
            'high_correlations': high_corr,
            'passed': len(high_corr) == 0
        }
    
    def _check_distributions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Check for significant changes in feature distributions."""
        numeric_df = df.select_dtypes(include=[np.number])
        distribution_stats = {}
        
        for column in numeric_df.columns:
            stats = {
                'mean': numeric_df[column].mean(),
                'std': numeric_df[column].std(),
                'skew': numeric_df[column].skew(),
                'kurtosis': numeric_df[column].kurtosis()
            }
            distribution_stats[column] = stats
        
        return {
            'distribution_stats': distribution_stats,
            'passed': True  # No specific failure criteria for distributions
        } 