"""
Trend detection module.
"""

from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class TrendDetector:
    """Trend detection processor."""
    
    def __init__(self, config: Config):
        """Initialize detector."""
        self.config = config
        self.trends = {}
    
    def detect_trends(
        self,
        df: pd.DataFrame,
        metric_column: str,
        date_column: str = 'date',
        window: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Detect trends in time series.
        
        Args:
            df: Input DataFrame
            metric_column: Metric column name
            date_column: Date column name
            window: Rolling window size
        
        Returns:
            Dictionary with trend information
        """
        try:
            # Ensure datetime
            df[date_column] = pd.to_datetime(df[date_column])
            df = df.sort_values(date_column)
            
            # Get window size
            if not window:
                window = self.config.analytics.get('trend_window', 7)
            
            # Calculate basic statistics
            current = df[metric_column].iloc[-1]
            previous = df[metric_column].iloc[-2]
            change = (current - previous) / previous if previous != 0 else 0
            
            # Calculate moving average
            ma = df[metric_column].rolling(
                window=window,
                min_periods=1
            ).mean()
            
            # Detect trend direction
            trend = self._detect_direction(
                df[metric_column].values,
                ma.values
            )
            
            # Calculate seasonality
            seasonality = self._detect_seasonality(
                df[metric_column].values
            )
            
            # Detect anomalies
            anomalies = self._detect_anomalies(
                df[metric_column].values,
                ma.values
            )
            
            # Prepare results
            results = {
                'metric': metric_column,
                'current_value': current,
                'previous_value': previous,
                'change': change,
                'trend': trend,
                'seasonality': seasonality,
                'anomalies': anomalies,
                'statistics': {
                    'mean': df[metric_column].mean(),
                    'std': df[metric_column].std(),
                    'min': df[metric_column].min(),
                    'max': df[metric_column].max()
                }
            }
            
            logger.info(f"Detected trends for {metric_column}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to detect trends: {e}")
            raise
    
    def _detect_direction(
        self,
        values: np.ndarray,
        ma_values: np.ndarray
    ) -> Dict[str, Any]:
        """Detect trend direction."""
        try:
            # Linear regression
            x = np.arange(len(values))
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                x,
                values
            )
            
            # Determine direction
            if p_value < 0.05:  # Statistically significant
                direction = 'up' if slope > 0 else 'down'
                strength = abs(r_value)
            else:
                direction = 'stable'
                strength = 0
            
            return {
                'direction': direction,
                'strength': strength,
                'slope': slope,
                'p_value': p_value,
                'r_squared': r_value ** 2
            }
            
        except Exception as e:
            logger.error(f"Failed to detect direction: {e}")
            raise
    
    def _detect_seasonality(
        self,
        values: np.ndarray
    ) -> Dict[str, Any]:
        """Detect seasonality."""
        try:
            # Calculate autocorrelation
            acf = pd.Series(values).autocorr()
            
            # Check for weekly pattern
            weekly = len(values) >= 14  # Need at least 2 weeks
            if weekly:
                weekly_corr = np.corrcoef(
                    values[7:],
                    values[:-7]
                )[0, 1]
            else:
                weekly_corr = 0
            
            return {
                'autocorrelation': acf,
                'weekly_correlation': weekly_corr,
                'is_seasonal': abs(weekly_corr) > 0.7
            }
            
        except Exception as e:
            logger.error(f"Failed to detect seasonality: {e}")
            raise
    
    def _detect_anomalies(
        self,
        values: np.ndarray,
        ma_values: np.ndarray
    ) -> Dict[str, Any]:
        """Detect anomalies."""
        try:
            # Calculate z-scores
            z_scores = np.abs(
                (values - np.mean(values)) / np.std(values)
            )
            
            # Find anomalies
            threshold = self.config.analytics.get('anomaly_threshold', 3)
            anomaly_indices = np.where(z_scores > threshold)[0]
            
            return {
                'count': len(anomaly_indices),
                'indices': anomaly_indices.tolist(),
                'values': values[anomaly_indices].tolist(),
                'z_scores': z_scores[anomaly_indices].tolist()
            }
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            raise 