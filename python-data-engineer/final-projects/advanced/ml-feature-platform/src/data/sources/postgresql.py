"""
PostgreSQL data source for feature extraction.
"""

from typing import Dict, List, Any
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)

class PostgreSQLSource:
    """PostgreSQL data source for feature extraction."""
    
    def __init__(self, connection_string: str):
        """Initialize PostgreSQL source."""
        self.engine = create_engine(connection_string)
        self.metrics = MetricsCollector()
    
    def extract_customer_features(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Extract customer features from PostgreSQL.
        
        Args:
            start_date: Start date for feature extraction
            end_date: End date for feature extraction
            
        Returns:
            DataFrame with customer features
        """
        try:
            self.metrics.start_operation("extract_customer_features")
            
            query = """
            SELECT 
                customer_id,
                COUNT(order_id) as total_orders,
                SUM(order_amount) as total_amount,
                AVG(order_amount) as avg_order_value,
                MAX(order_amount) as max_order_value,
                MIN(order_amount) as min_order_value,
                COUNT(DISTINCT product_id) as unique_products
            FROM orders
            WHERE order_date BETWEEN :start_date AND :end_date
            GROUP BY customer_id
            """
            
            df = pd.read_sql_query(
                text(query),
                self.engine,
                params={
                    "start_date": start_date,
                    "end_date": end_date
                }
            )
            
            duration = self.metrics.end_operation("extract_customer_features")
            logger.info(f"Extracted {len(df)} customer features in {duration:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract customer features: {e}")
            self.metrics.record_error("extract_customer_features")
            raise
    
    def extract_product_features(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Extract product features from PostgreSQL.
        
        Args:
            start_date: Start date for feature extraction
            end_date: End date for feature extraction
            
        Returns:
            DataFrame with product features
        """
        try:
            self.metrics.start_operation("extract_product_features")
            
            query = """
            SELECT 
                product_id,
                COUNT(order_id) as total_sales,
                SUM(quantity) as total_quantity,
                AVG(price) as avg_price,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_date BETWEEN :start_date AND :end_date
            GROUP BY product_id
            """
            
            df = pd.read_sql_query(
                text(query),
                self.engine,
                params={
                    "start_date": start_date,
                    "end_date": end_date
                }
            )
            
            duration = self.metrics.end_operation("extract_product_features")
            logger.info(f"Extracted {len(df)} product features in {duration:.2f}s")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract product features: {e}")
            self.metrics.record_error("extract_product_features")
            raise 