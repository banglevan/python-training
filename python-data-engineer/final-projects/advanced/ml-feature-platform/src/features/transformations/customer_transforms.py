"""
Customer feature transformations.
"""

from typing import Dict, Any
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.monitoring.metrics import MetricsCollector

class CustomerFeatureTransformer:
    """Transform customer features."""
    
    def __init__(self):
        """Initialize transformer."""
        self.metrics = MetricsCollector()
    
    def compute_order_features(
        self,
        orders_df: pd.DataFrame,
        customer_id: int
    ) -> Dict[str, Any]:
        """
        Compute order-based features for a customer.
        
        Args:
            orders_df: DataFrame with customer orders
            customer_id: Customer identifier
            
        Returns:
            Dictionary of computed features
        """
        try:
            self.metrics.start_operation("compute_order_features")
            
            # Filter customer orders
            customer_orders = orders_df[
                orders_df['customer_id'] == customer_id
            ]
            
            if len(customer_orders) == 0:
                return {
                    'total_orders': 0,
                    'total_amount': 0.0,
                    'avg_order_value': 0.0,
                    'days_since_last_order': None,
                    'order_frequency': 0.0
                }
            
            # Compute features
            features = {
                'total_orders': len(customer_orders),
                'total_amount': customer_orders['order_amount'].sum(),
                'avg_order_value': customer_orders['order_amount'].mean(),
                'days_since_last_order': (
                    datetime.now() - customer_orders['order_date'].max()
                ).days,
                'order_frequency': self._compute_order_frequency(customer_orders)
            }
            
            duration = self.metrics.end_operation("compute_order_features")
            
            return features
            
        except Exception as e:
            self.metrics.record_error("compute_order_features")
            raise
    
    def compute_product_affinity(
        self,
        orders_df: pd.DataFrame,
        customer_id: int
    ) -> Dict[str, Any]:
        """
        Compute product affinity features for a customer.
        
        Args:
            orders_df: DataFrame with customer orders
            customer_id: Customer identifier
            
        Returns:
            Dictionary of computed features
        """
        try:
            self.metrics.start_operation("compute_product_affinity")
            
            # Filter customer orders
            customer_orders = orders_df[
                orders_df['customer_id'] == customer_id
            ]
            
            if len(customer_orders) == 0:
                return {
                    'favorite_category': None,
                    'category_diversity': 0.0,
                    'repeat_purchase_rate': 0.0
                }
            
            # Compute features
            features = {
                'favorite_category': self._get_favorite_category(customer_orders),
                'category_diversity': self._compute_category_diversity(customer_orders),
                'repeat_purchase_rate': self._compute_repeat_purchase_rate(customer_orders)
            }
            
            duration = self.metrics.end_operation("compute_product_affinity")
            
            return features
            
        except Exception as e:
            self.metrics.record_error("compute_product_affinity")
            raise
    
    def _compute_order_frequency(self, orders_df: pd.DataFrame) -> float:
        """Compute average order frequency in orders per month."""
        if len(orders_df) <= 1:
            return 0.0
            
        date_range = (
            orders_df['order_date'].max() - orders_df['order_date'].min()
        ).days
        
        if date_range == 0:
            return 0.0
            
        return (len(orders_df) / (date_range / 30.0))
    
    def _get_favorite_category(self, orders_df: pd.DataFrame) -> str:
        """Get customer's favorite product category."""
        return orders_df['category'].mode().iloc[0]
    
    def _compute_category_diversity(self, orders_df: pd.DataFrame) -> float:
        """Compute diversity of product categories purchased."""
        category_counts = orders_df['category'].value_counts()
        return len(category_counts) / len(orders_df)
    
    def _compute_repeat_purchase_rate(self, orders_df: pd.DataFrame) -> float:
        """Compute rate of repeat product purchases."""
        product_counts = orders_df['product_id'].value_counts()
        return (product_counts > 1).sum() / len(product_counts) 