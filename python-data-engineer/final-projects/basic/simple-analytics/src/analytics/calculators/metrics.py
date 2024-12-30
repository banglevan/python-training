"""
Metrics calculation module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class MetricsCalculator:
    """Metrics calculation engine."""
    
    def __init__(self, config: Config):
        """Initialize calculator."""
        self.config = config
    
    def calculate_growth(
        self,
        current: float,
        previous: float
    ) -> float:
        """
        Calculate growth rate.
        
        Args:
            current: Current value
            previous: Previous value
        
        Returns:
            Growth rate
        """
        try:
            if previous == 0:
                return 0.0
            return (current - previous) / previous
        except Exception as e:
            logger.error(f"Failed to calculate growth: {e}")
            raise
    
    def calculate_retention(
        self,
        df: pd.DataFrame,
        customer_col: str,
        date_col: str,
        window_days: int = 30
    ) -> float:
        """
        Calculate customer retention rate.
        
        Args:
            df: Input DataFrame
            customer_col: Customer ID column
            date_col: Date column
            window_days: Time window in days
        
        Returns:
            Retention rate
        """
        try:
            df[date_col] = pd.to_datetime(df[date_col])
            end_date = df[date_col].max()
            start_date = end_date - timedelta(days=window_days)
            
            # Previous period customers
            previous_customers = set(
                df[
                    (df[date_col] >= start_date - timedelta(days=window_days)) &
                    (df[date_col] < start_date)
                ][customer_col]
            )
            
            # Current period retained customers
            retained_customers = set(
                df[
                    (df[date_col] >= start_date) &
                    (df[date_col] <= end_date)
                ][customer_col]
            ).intersection(previous_customers)
            
            if not previous_customers:
                return 0.0
                
            return len(retained_customers) / len(previous_customers)
            
        except Exception as e:
            logger.error(f"Failed to calculate retention: {e}")
            raise
    
    def calculate_moving_average(
        self,
        series: pd.Series,
        window: int = 7,
        min_periods: int = 1
    ) -> pd.Series:
        """
        Calculate moving average.
        
        Args:
            series: Input series
            window: Window size
            min_periods: Minimum periods
        
        Returns:
            Moving average series
        """
        try:
            return series.rolling(
                window=window,
                min_periods=min_periods
            ).mean()
        except Exception as e:
            logger.error(f"Failed to calculate moving average: {e}")
            raise
    
    def calculate_percentile(
        self,
        series: pd.Series,
        percentile: float
    ) -> float:
        """
        Calculate percentile value.
        
        Args:
            series: Input series
            percentile: Percentile (0-1)
        
        Returns:
            Percentile value
        """
        try:
            return series.quantile(percentile)
        except Exception as e:
            logger.error(f"Failed to calculate percentile: {e}")
            raise
    
    def calculate_segment_metrics(
        self,
        df: pd.DataFrame,
        segment_col: str,
        metric_col: str
    ) -> Dict[str, Dict[str, float]]:
        """
        Calculate metrics by segment.
        
        Args:
            df: Input DataFrame
            segment_col: Segment column
            metric_col: Metric column
        
        Returns:
            Dictionary of segment metrics
        """
        try:
            metrics = {}
            for segment in df[segment_col].unique():
                segment_data = df[df[segment_col] == segment][metric_col]
                metrics[segment] = {
                    'count': len(segment_data),
                    'sum': segment_data.sum(),
                    'mean': segment_data.mean(),
                    'median': segment_data.median(),
                    'std': segment_data.std()
                }
            return metrics
        except Exception as e:
            logger.error(f"Failed to calculate segment metrics: {e}")
            raise 