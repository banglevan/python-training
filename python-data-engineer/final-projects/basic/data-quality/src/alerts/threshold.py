"""
Alert threshold management module.
"""

from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class ThresholdManager:
    """Alert threshold management."""
    
    def __init__(self, config: Config):
        """Initialize manager."""
        self.config = config
        self.thresholds = {}
        self._load_thresholds()
    
    def _load_thresholds(self):
        """Load threshold configurations."""
        try:
            self.thresholds = {
                level: config['threshold']
                for level, config in self.config.alerts['levels'].items()
            }
            
            # Add dynamic thresholds if configured
            if 'dynamic_thresholds' in self.config.alerts:
                self._calculate_dynamic_thresholds()
                
            logger.info("Loaded threshold configurations")
            
        except Exception as e:
            logger.error(f"Failed to load thresholds: {e}")
            raise
    
    def _calculate_dynamic_thresholds(self):
        """Calculate dynamic thresholds based on historical data."""
        try:
            for metric, config in self.config.alerts['dynamic_thresholds'].items():
                # Load historical data
                history = self._load_metric_history(metric)
                if not history:
                    continue
                
                # Calculate statistics
                values = pd.Series(history)
                mean = values.mean()
                std = values.std()
                
                # Set dynamic thresholds
                self.thresholds[f"{metric}_critical"] = mean - (3 * std)
                self.thresholds[f"{metric}_warning"] = mean - (2 * std)
                self.thresholds[f"{metric}_info"] = mean - std
                
        except Exception as e:
            logger.error(f"Failed to calculate dynamic thresholds: {e}")
            raise
    
    def check_threshold(
        self,
        metric: str,
        value: float
    ) -> Optional[str]:
        """
        Check value against thresholds.
        
        Args:
            metric: Metric name
            value: Metric value
        
        Returns:
            Alert level if threshold breached, None otherwise
        """
        try:
            # Check static thresholds
            for level, threshold in self.thresholds.items():
                if value < threshold:
                    return level
            
            # Check dynamic thresholds
            metric_thresholds = {
                k: v for k, v in self.thresholds.items()
                if k.startswith(f"{metric}_")
            }
            
            for level, threshold in metric_thresholds.items():
                if value < threshold:
                    return level.replace(f"{metric}_", "")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to check threshold: {e}")
            raise
    
    def get_threshold(
        self,
        metric: str,
        level: str
    ) -> float:
        """
        Get threshold value.
        
        Args:
            metric: Metric name
            level: Alert level
        
        Returns:
            Threshold value
        """
        try:
            # Check dynamic threshold
            dynamic_key = f"{metric}_{level}"
            if dynamic_key in self.thresholds:
                return self.thresholds[dynamic_key]
            
            # Return static threshold
            return self.thresholds[level]
            
        except Exception as e:
            logger.error(f"Failed to get threshold: {e}")
            raise
    
    def _load_metric_history(
        self,
        metric: str
    ) -> list:
        """Load historical metric values."""
        try:
            history_file = f"metrics/{metric}_history.csv"
            df = pd.read_csv(history_file)
            return df[metric].tolist()
        except Exception:
            return [] 