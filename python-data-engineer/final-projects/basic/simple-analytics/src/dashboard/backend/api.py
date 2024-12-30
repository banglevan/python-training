"""
Dashboard API module.
"""

from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime, timedelta
import logging
from src.utils.config import Config
from src.analytics.processors.metrics import MetricsProcessor
from src.analytics.processors.trends import TrendDetector

logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="Analytics Dashboard API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
config = Config()
metrics_processor = MetricsProcessor(config)
trend_detector = TrendDetector(config)

@app.get("/api/metrics/sales")
async def get_sales_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    group_by: Optional[str] = None
) -> Dict[str, Any]:
    """Get sales metrics."""
    try:
        # Load data
        query = """
            SELECT s.*, p.category, p.subcategory
            FROM fact_sales s
            JOIN dim_products p ON s.product_id = p.product_id
            WHERE 1=1
        """
        params = {}
        
        if start_date:
            query += " AND sale_date >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND sale_date <= :end_date"
            params['end_date'] = end_date
        
        df = pd.read_sql(query, config.warehouse['engine'], params=params)
        
        # Calculate metrics
        group_cols = ['category'] if group_by == 'category' else None
        metrics = metrics_processor.process_sales_metrics(df, group_by=group_cols)
        
        # Detect trends
        trends = {}
        for col in ['total_revenue', 'transaction_count']:
            trends[col] = trend_detector.detect_trends(
                metrics,
                col,
                'sale_date'
            )
        
        return {
            'metrics': metrics.to_dict('records'),
            'trends': trends
        }
        
    except Exception as e:
        logger.error(f"Failed to get sales metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/customers")
async def get_customer_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """Get customer metrics."""
    try:
        # Load data
        query = """
            SELECT s.*, c.segment
            FROM fact_sales s
            JOIN dim_customers c ON s.customer_id = c.customer_id
            WHERE 1=1
        """
        params = {}
        
        if start_date:
            query += " AND sale_date >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND sale_date <= :end_date"
            params['end_date'] = end_date
        
        df = pd.read_sql(query, config.warehouse['engine'], params=params)
        
        # Calculate metrics
        metrics = metrics_processor.process_customer_metrics(df)
        
        # Detect trends
        trends = {}
        for col in ['active_customers', 'new_customers', 'retention_rate']:
            trends[col] = trend_detector.detect_trends(metrics, col)
        
        return {
            'metrics': metrics.to_dict('records'),
            'trends': trends
        }
        
    except Exception as e:
        logger.error(f"Failed to get customer metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/metrics/products")
async def get_product_metrics(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    category: Optional[str] = None
) -> Dict[str, Any]:
    """Get product metrics."""
    try:
        # Load data
        query = """
            SELECT s.*, p.category, p.subcategory
            FROM fact_sales s
            JOIN dim_products p ON s.product_id = p.product_id
            WHERE 1=1
        """
        params = {}
        
        if start_date:
            query += " AND sale_date >= :start_date"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND sale_date <= :end_date"
            params['end_date'] = end_date
        
        if category:
            query += " AND p.category = :category"
            params['category'] = category
        
        df = pd.read_sql(query, config.warehouse['engine'], params=params)
        
        # Calculate metrics
        metrics = metrics_processor.process_product_metrics(df)
        
        # Get top products
        top_products = (
            metrics.groupby('product_id')['revenue']
            .sum()
            .sort_values(ascending=False)
            .head(10)
        )
        
        return {
            'metrics': metrics.to_dict('records'),
            'top_products': top_products.to_dict()
        }
        
    except Exception as e:
        logger.error(f"Failed to get product metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 