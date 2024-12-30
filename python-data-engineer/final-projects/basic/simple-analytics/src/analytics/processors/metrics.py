"""
Metrics processing module.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from src.utils.config import Config

logger = logging.getLogger(__name__)

class MetricsProcessor:
    """Metrics calculation processor."""
    
    def __init__(self, config: Config):
        """Initialize processor."""
        self.config = config
        self.metrics = {}
    
    def process_sales_metrics(
        self,
        df: pd.DataFrame,
        date_column: str = 'sale_date',
        group_by: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Process sales metrics.
        
        Args:
            df: Sales DataFrame
            date_column: Date column name
            group_by: Additional grouping columns
        
        Returns:
            DataFrame with calculated metrics
        """
        try:
            # Ensure datetime
            df[date_column] = pd.to_datetime(df[date_column])
            
            # Base grouping
            grouping = [date_column]
            if group_by:
                grouping.extend(group_by)
            
            # Calculate metrics
            metrics = df.groupby(grouping).agg({
                'sale_id': 'count',
                'quantity': 'sum',
                'unit_price': 'mean',
                'total_amount': ['sum', 'mean'],
                'customer_id': 'nunique'
            })
            
            # Rename columns
            metrics.columns = [
                'transaction_count',
                'total_units',
                'avg_unit_price',
                'total_revenue',
                'avg_transaction_value',
                'unique_customers'
            ]
            
            # Reset index
            metrics = metrics.reset_index()
            
            logger.info(f"Calculated sales metrics: {len(metrics)} rows")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to process sales metrics: {e}")
            raise
    
    def process_customer_metrics(
        self,
        df: pd.DataFrame,
        date_column: str = 'sale_date'
    ) -> pd.DataFrame:
        """
        Process customer metrics.
        
        Args:
            df: Sales DataFrame
            date_column: Date column name
        
        Returns:
            DataFrame with calculated metrics
        """
        try:
            # Ensure datetime
            df[date_column] = pd.to_datetime(df[date_column])
            
            # Customer first purchase
            first_purchase = df.groupby('customer_id')[date_column].min()
            
            # Daily metrics
            daily = df.groupby(date_column).agg({
                'customer_id': 'nunique',
                'total_amount': 'sum'
            })
            
            # New customers per day
            new_customers = df.groupby([date_column, 'customer_id']).size()
            new_customers = new_customers.reset_index()
            new_customers = new_customers.merge(
                first_purchase.reset_index(),
                on='customer_id'
            )
            new_customers = new_customers[
                new_customers[f"{date_column}_x"] == 
                new_customers[f"{date_column}_y"]
            ]
            new_daily = new_customers.groupby(f"{date_column}_x").size()
            
            # Combine metrics
            metrics = pd.DataFrame({
                'date': daily.index,
                'active_customers': daily['customer_id'],
                'new_customers': new_daily,
                'revenue': daily['total_amount']
            })
            
            # Calculate retention
            metrics['retention_rate'] = (
                metrics['active_customers'] - metrics['new_customers']
            ) / metrics['active_customers'].shift(1)
            
            logger.info(f"Calculated customer metrics: {len(metrics)} rows")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to process customer metrics: {e}")
            raise
    
    def process_product_metrics(
        self,
        df: pd.DataFrame,
        date_column: str = 'sale_date'
    ) -> pd.DataFrame:
        """
        Process product metrics.
        
        Args:
            df: Sales DataFrame
            date_column: Date column name
        
        Returns:
            DataFrame with calculated metrics
        """
        try:
            # Ensure datetime
            df[date_column] = pd.to_datetime(df[date_column])
            
            # Calculate metrics by product
            metrics = df.groupby(['product_id', date_column]).agg({
                'quantity': 'sum',
                'total_amount': 'sum',
                'sale_id': 'count',
                'customer_id': 'nunique'
            })
            
            # Rename columns
            metrics.columns = [
                'units_sold',
                'revenue',
                'transaction_count',
                'unique_customers'
            ]
            
            # Calculate moving averages
            window = self.config.analytics.get('moving_average_window', 7)
            for col in metrics.columns:
                metrics[f"{col}_ma{window}"] = metrics[col].rolling(
                    window=window,
                    min_periods=1
                ).mean()
            
            # Reset index
            metrics = metrics.reset_index()
            
            logger.info(f"Calculated product metrics: {len(metrics)} rows")
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to process product metrics: {e}")
            raise
    
    def save_metrics(
        self,
        metrics: pd.DataFrame,
        table_name: str,
        engine
    ) -> None:
        """
        Save metrics to database.
        
        Args:
            metrics: Metrics DataFrame
            table_name: Target table name
            engine: SQLAlchemy engine
        """
        try:
            metrics['created_at'] = datetime.now()
            metrics.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Saved metrics to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to save metrics: {e}")
            raise 