"""
Product feature transformations.
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.monitoring.metrics import MetricsCollector

class ProductFeatureTransformer:
    """Transform product features."""
    
    def __init__(self):
        """Initialize transformer."""
        self.metrics = MetricsCollector()
    
    def compute_sales_features(
        self,
        orders_df: pd.DataFrame,
        product_id: str
    ) -> Dict[str, Any]:
        """
        Compute sales-based features for a product.
        
        Args:
            orders_df: DataFrame with product orders
            product_id: Product identifier
            
        Returns:
            Dictionary of computed features
        """
        try:
            self.metrics.start_operation("compute_sales_features")
            
            # Filter product orders
            product_orders = orders_df[
                orders_df['product_id'] == product_id
            ]
            
            if len(product_orders) == 0:
                return {
                    'total_sales': 0,
                    'total_revenue': 0.0,
                    'avg_price': 0.0,
                    'sales_velocity': 0.0
                }
            
            # Compute features
            features = {
                'total_sales': len(product_orders),
                'total_revenue': product_orders['order_amount'].sum(),
                'avg_price': product_orders['price'].mean(),
                'sales_velocity': self._compute_sales_velocity(product_orders)
            }
            
            duration = self.metrics.end_operation("compute_sales_features")
            
            return features
            
        except Exception as e:
            self.metrics.record_error("compute_sales_features")
            raise
    
    def compute_performance_features(
        self,
        orders_df: pd.DataFrame,
        returns_df: pd.DataFrame,
        reviews_df: pd.DataFrame,
        product_id: str
    ) -> Dict[str, Any]:
        """
        Compute performance-based features for a product.
        
        Args:
            orders_df: DataFrame with product orders
            returns_df: DataFrame with product returns
            reviews_df: DataFrame with product reviews
            product_id: Product identifier
            
        Returns:
            Dictionary of computed features
        """
        try:
            self.metrics.start_operation("compute_performance_features")
            
            features = {
                'return_rate': self._compute_return_rate(
                    orders_df, returns_df, product_id
                ),
                'review_score': self._compute_review_score(
                    reviews_df, product_id
                ),
                'review_count': self._get_review_count(
                    reviews_df, product_id
                )
            }
            
            duration = self.metrics.end_operation("compute_performance_features")
            
            return features
            
        except Exception as e:
            self.metrics.record_error("compute_performance_features")
            raise
    
    def _compute_sales_velocity(self, orders_df: pd.DataFrame) -> float:
        """Compute average sales velocity (units per day)."""
        if len(orders_df) <= 1:
            return 0.0
            
        date_range = (
            orders_df['order_date'].max() - orders_df['order_date'].min()
        ).days
        
        if date_range == 0:
            return 0.0
            
        return len(orders_df) / date_range
    
    def _compute_return_rate(
        self,
        orders_df: pd.DataFrame,
        returns_df: pd.DataFrame,
        product_id: str
    ) -> float:
        """Compute product return rate."""
        total_orders = len(
            orders_df[orders_df['product_id'] == product_id]
        )
        
        if total_orders == 0:
            return 0.0
            
        total_returns = len(
            returns_df[returns_df['product_id'] == product_id]
        )
        
        return total_returns / total_orders
    
    def _compute_review_score(
        self,
        reviews_df: pd.DataFrame,
        product_id: str
    ) -> float:
        """Compute average review score."""
        product_reviews = reviews_df[
            reviews_df['product_id'] == product_id
        ]
        
        if len(product_reviews) == 0:
            return 0.0
            
        return product_reviews['rating'].mean()
    
    def _get_review_count(
        self,
        reviews_df: pd.DataFrame,
        product_id: str
    ) -> int:
        """Get total number of reviews."""
        return len(reviews_df[reviews_df['product_id'] == product_id]) 