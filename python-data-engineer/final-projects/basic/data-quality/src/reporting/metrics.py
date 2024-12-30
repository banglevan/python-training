"""
Quality metrics calculation module.
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class QualityMetrics:
    """Quality metrics calculation management."""
    
    def __init__(self, config: Config):
        """Initialize metrics calculator."""
        self.config = config
        self.metrics = {}
    
    def calculate_dataset_metrics(
        self,
        check_results: List[Dict[str, Any]]
    ) -> Dict[str, float]:
        """
        Calculate quality metrics for dataset.
        
        Args:
            check_results: Quality check results
        
        Returns:
            Dictionary of metrics
        """
        try:
            # Calculate scores by check type
            scores_by_type = {}
            for result in check_results:
                check_type = result['type']
                if check_type not in scores_by_type:
                    scores_by_type[check_type] = []
                scores_by_type[check_type].append(result['score'])
            
            # Calculate average scores
            metrics = {
                f"{check_type}_score": np.mean(scores)
                for check_type, scores in scores_by_type.items()
            }
            
            # Calculate overall quality score
            weights = self.config.metrics['weights']
            overall_score = sum(
                metrics.get(f"{check_type}_score", 0) * weight
                for check_type, weight in weights.items()
            )
            
            metrics['quality_score'] = overall_score
            metrics['timestamp'] = datetime.now().isoformat()
            
            self.metrics = metrics
            logger.info(f"Calculated quality metrics: {metrics}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate metrics: {e}")
            raise
    
    def calculate_trend_metrics(
        self,
        historical_metrics: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate trend metrics.
        
        Args:
            historical_metrics: Historical metrics data
        
        Returns:
            Trend metrics dictionary
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame(historical_metrics)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Calculate trends
            trends = {}
            for col in df.columns:
                if col != 'timestamp' and df[col].dtype in ['float64', 'int64']:
                    # Calculate change
                    current = df[col].iloc[-1]
                    previous = df[col].iloc[-2] if len(df) > 1 else current
                    change = current - previous
                    
                    # Calculate trend direction
                    trend = 'up' if change > 0 else 'down' if change < 0 else 'stable'
                    
                    trends[col] = {
                        'current': current,
                        'previous': previous,
                        'change': change,
                        'trend': trend
                    }
            
            logger.info(f"Calculated trend metrics: {trends}")
            return trends
            
        except Exception as e:
            logger.error(f"Failed to calculate trends: {e}")
            raise
    
    def get_metrics(self) -> Dict[str, float]:
        """Get calculated metrics."""
        return self.metrics 