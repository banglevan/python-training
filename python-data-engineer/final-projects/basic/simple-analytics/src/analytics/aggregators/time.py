"""
Time-based aggregation module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class TimeAggregator:
    """Time-based data aggregation."""
    
    def __init__(self, config: Config):
        """Initialize aggregator."""
        self.config = config
    
    def aggregate_daily(
        self,
        df: pd.DataFrame,
        date_col: str,
        metrics: List[Dict[str, str]],
        group_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by day.
        
        Args:
            df: Input DataFrame
            date_col: Date column
            metrics: List of metrics to calculate
            group_cols: Additional grouping columns
        
        Returns:
            Aggregated DataFrame
        """
        try:
            # Ensure datetime
            df[date_col] = pd.to_datetime(df[date_col])
            
            # Set up grouping
            group_cols = group_cols or []
            grouping = [pd.Grouper(key=date_col, freq='D')] + group_cols
            
            # Create aggregation dict
            agg_dict = {}
            for metric in metrics:
                agg_dict[f"{metric['name']}"] = metric['type']
            
            # Aggregate
            result = df.groupby(grouping).agg(agg_dict)
            
            # Reset index
            result = result.reset_index()
            
            logger.info(f"Aggregated {len(metrics)} metrics by day")
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate daily: {e}")
            raise
    
    def aggregate_weekly(
        self,
        df: pd.DataFrame,
        date_col: str,
        metrics: List[Dict[str, str]],
        group_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by week.
        
        Args:
            df: Input DataFrame
            date_col: Date column
            metrics: List of metrics to calculate
            group_cols: Additional grouping columns
        
        Returns:
            Aggregated DataFrame
        """
        try:
            # Ensure datetime
            df[date_col] = pd.to_datetime(df[date_col])
            
            # Set up grouping
            group_cols = group_cols or []
            grouping = [pd.Grouper(key=date_col, freq='W')] + group_cols
            
            # Create aggregation dict
            agg_dict = {}
            for metric in metrics:
                agg_dict[f"{metric['name']}"] = metric['type']
            
            # Aggregate
            result = df.groupby(grouping).agg(agg_dict)
            
            # Reset index
            result = result.reset_index()
            
            logger.info(f"Aggregated {len(metrics)} metrics by week")
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate weekly: {e}")
            raise
    
    def aggregate_monthly(
        self,
        df: pd.DataFrame,
        date_col: str,
        metrics: List[Dict[str, str]],
        group_cols: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by month.
        
        Args:
            df: Input DataFrame
            date_col: Date column
            metrics: List of metrics to calculate
            group_cols: Additional grouping columns
        
        Returns:
            Aggregated DataFrame
        """
        try:
            # Ensure datetime
            df[date_col] = pd.to_datetime(df[date_col])
            
            # Set up grouping
            group_cols = group_cols or []
            grouping = [pd.Grouper(key=date_col, freq='M')] + group_cols
            
            # Create aggregation dict
            agg_dict = {}
            for metric in metrics:
                agg_dict[f"{metric['name']}"] = metric['type']
            
            # Aggregate
            result = df.groupby(grouping).agg(agg_dict)
            
            # Reset index
            result = result.reset_index()
            
            logger.info(f"Aggregated {len(metrics)} metrics by month")
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate monthly: {e}")
            raise 