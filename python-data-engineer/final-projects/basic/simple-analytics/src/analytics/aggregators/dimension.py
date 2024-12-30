"""
Dimension-based aggregation module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class DimensionAggregator:
    """Dimension-based data aggregation."""
    
    def __init__(self, config: Config):
        """Initialize aggregator."""
        self.config = config
    
    def aggregate_by_dimension(
        self,
        df: pd.DataFrame,
        dimension: str,
        metrics: List[Dict[str, str]],
        filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Aggregate data by dimension.
        
        Args:
            df: Input DataFrame
            dimension: Dimension column
            metrics: List of metrics to calculate
            filters: Dimension filters
        
        Returns:
            Aggregated DataFrame
        """
        try:
            # Apply filters
            if filters:
                for col, value in filters.items():
                    if isinstance(value, (list, tuple)):
                        df = df[df[col].isin(value)]
                    else:
                        df = df[df[col] == value]
            
            # Create aggregation dict
            agg_dict = {}
            for metric in metrics:
                agg_dict[f"{metric['name']}"] = metric['type']
            
            # Aggregate
            result = df.groupby(dimension).agg(agg_dict)
            
            # Reset index
            result = result.reset_index()
            
            # Sort by first metric
            if metrics:
                result = result.sort_values(
                    metrics[0]['name'],
                    ascending=False
                )
            
            logger.info(f"Aggregated {len(metrics)} metrics by {dimension}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate by dimension: {e}")
            raise
    
    def calculate_dimension_metrics(
        self,
        df: pd.DataFrame,
        dimension: str,
        metric: str,
        top_n: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Calculate dimension metrics.
        
        Args:
            df: Input DataFrame
            dimension: Dimension column
            metric: Metric column
            top_n: Number of top values
        
        Returns:
            Dictionary of dimension metrics
        """
        try:
            # Group by dimension
            grouped = df.groupby(dimension)[metric]
            
            # Calculate metrics
            metrics = {
                'total': grouped.sum(),
                'average': grouped.mean(),
                'count': grouped.count(),
                'min': grouped.min(),
                'max': grouped.max()
            }
            
            # Get top values
            if top_n:
                top_values = (
                    metrics['total']
                    .sort_values(ascending=False)
                    .head(top_n)
                )
                metrics['top_values'] = top_values.to_dict()
            
            # Calculate percentages
            total = metrics['total'].sum()
            metrics['percentages'] = (metrics['total'] / total).to_dict()
            
            logger.info(f"Calculated metrics for dimension: {dimension}")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate dimension metrics: {e}")
            raise
    
    def pivot_dimensions(
        self,
        df: pd.DataFrame,
        dimensions: List[str],
        metric: str,
        agg_func: str = 'sum'
    ) -> pd.DataFrame:
        """
        Create pivot table for dimensions.
        
        Args:
            df: Input DataFrame
            dimensions: List of dimension columns
            metric: Metric column
            agg_func: Aggregation function
        
        Returns:
            Pivot table DataFrame
        """
        try:
            if len(dimensions) != 2:
                raise ValueError("Pivot requires exactly 2 dimensions")
            
            pivot = pd.pivot_table(
                df,
                values=metric,
                index=dimensions[0],
                columns=dimensions[1],
                aggfunc=agg_func,
                fill_value=0
            )
            
            logger.info(f"Created pivot table for {dimensions}")
            return pivot
            
        except Exception as e:
            logger.error(f"Failed to create pivot table: {e}")
            raise 