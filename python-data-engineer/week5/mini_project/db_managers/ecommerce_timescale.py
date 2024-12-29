"""
E-commerce TimescaleDB Manager
----------------------------

PURPOSE:
Handles all time-series analytics data:
1. Sales Analytics
   - Revenue metrics
   - Order volumes
   - Product performance

2. Inventory Tracking
   - Stock levels
   - Reorder patterns
   - Warehouse status

3. Customer Metrics
   - Purchase frequency
   - Average order value
   - Customer lifetime value

TABLES:
1. sales_metrics
   - Time-based sales data
   - Product performance
   - Revenue analysis

2. inventory_metrics
   - Stock level history
   - Reorder points
   - Warehouse capacity

3. customer_metrics
   - Purchase patterns
   - Customer segments
   - Retention rates
"""

from typing import Dict, List, Any, Union
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, timedelta
from .base_manager import BaseManager

logger = logging.getLogger(__name__)

class EcommerceTimescaleManager(BaseManager):
    """TimescaleDB manager for e-commerce analytics."""
    
    def _connect(self) -> None:
        """Initialize TimescaleDB connection."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.config.get('dbname', 'ecommerce'),
                user=self.config.get('user', 'postgres'),
                password=self.config.get('password', ''),
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 5432)
            )
            self.cur = self.conn.cursor(
                cursor_factory=RealDictCursor
            )
            
            # Initialize tables
            self._setup_tables()
            logger.info("E-commerce TimescaleDB initialized")
            
        except Exception as e:
            logger.error(f"TimescaleDB initialization error: {e}")
            raise
    
    def _setup_tables(self) -> None:
        """Create necessary tables and hypertables."""
        try:
            # Sales metrics table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS sales_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    product_id TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    quantity INT NOT NULL,
                    unit_price DECIMAL(10,2) NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL,
                    order_id TEXT NOT NULL,
                    store_id TEXT NOT NULL
                );
                
                SELECT create_hypertable(
                    'sales_metrics',
                    'time',
                    if_not_exists => TRUE
                );
            """)
            
            # Inventory metrics table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS inventory_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    product_id TEXT NOT NULL,
                    warehouse_id TEXT NOT NULL,
                    current_stock INT NOT NULL,
                    reorder_point INT NOT NULL,
                    restock_amount INT NOT NULL
                );
                
                SELECT create_hypertable(
                    'inventory_metrics',
                    'time',
                    if_not_exists => TRUE
                );
            """)
            
            # Customer metrics table
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS customer_metrics (
                    time TIMESTAMPTZ NOT NULL,
                    customer_id TEXT NOT NULL,
                    session_duration INT,
                    cart_value DECIMAL(10,2),
                    items_viewed INT,
                    conversion_status BOOLEAN
                );
                
                SELECT create_hypertable(
                    'customer_metrics',
                    'time',
                    if_not_exists => TRUE
                );
            """)
            
            # Create continuous aggregates
            self._setup_aggregates()
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Table setup error: {e}")
            raise
    
    def _setup_aggregates(self) -> None:
        """Setup continuous aggregates for analytics."""
        try:
            # Hourly sales aggregate
            self.cur.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_sales
                WITH (timescaledb.continuous) AS
                SELECT time_bucket('1 hour', time) AS bucket,
                       store_id,
                       SUM(total_amount) as revenue,
                       COUNT(*) as order_count,
                       COUNT(DISTINCT customer_id) as unique_customers
                FROM sales_metrics
                GROUP BY bucket, store_id;
                
                SELECT add_continuous_aggregate_policy('hourly_sales',
                    start_offset => INTERVAL '1 month',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour');
            """)
            
            # Daily inventory aggregate
            self.cur.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS daily_inventory
                WITH (timescaledb.continuous) AS
                SELECT time_bucket('1 day', time) AS bucket,
                       warehouse_id,
                       product_id,
                       AVG(current_stock) as avg_stock,
                       MIN(current_stock) as min_stock,
                       MAX(current_stock) as max_stock
                FROM inventory_metrics
                GROUP BY bucket, warehouse_id, product_id;
                
                SELECT add_continuous_aggregate_policy('daily_inventory',
                    start_offset => INTERVAL '3 months',
                    end_offset => INTERVAL '1 day',
                    schedule_interval => INTERVAL '1 day');
            """)
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Aggregate setup error: {e}")
            raise
    
    def record_sale(
        self,
        sale_data: Dict[str, Any]
    ) -> bool:
        """Record a new sale transaction."""
        try:
            self.cur.execute("""
                INSERT INTO sales_metrics (
                    time, product_id, customer_id, quantity,
                    unit_price, total_amount, order_id, store_id
                ) VALUES (
                    %(time)s, %(product_id)s, %(customer_id)s,
                    %(quantity)s, %(unit_price)s, %(total_amount)s,
                    %(order_id)s, %(store_id)s
                )
            """, sale_data)
            
            self.conn.commit()
            logger.info(f"Recorded sale: {sale_data['order_id']}")
            return True
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Sale recording error: {e}")
            raise
    
    def get_sales_analytics(
        self,
        start_time: datetime,
        end_time: datetime,
        group_by: str = 'hour'
    ) -> List[Dict[str, Any]]:
        """Get sales analytics for a time period."""
        try:
            self.cur.execute("""
                SELECT 
                    time_bucket(%(interval)s, time) AS period,
                    SUM(total_amount) as revenue,
                    COUNT(DISTINCT order_id) as orders,
                    COUNT(DISTINCT customer_id) as customers,
                    AVG(total_amount) as avg_order_value
                FROM sales_metrics
                WHERE time BETWEEN %(start)s AND %(end)s
                GROUP BY period
                ORDER BY period;
            """, {
                'interval': f'1 {group_by}',
                'start': start_time,
                'end': end_time
            })
            
            return self.cur.fetchall()
            
        except Exception as e:
            logger.error(f"Analytics query error: {e}")
            raise
    
    def get_inventory_status(
        self,
        warehouse_id: str = None
    ) -> List[Dict[str, Any]]:
        """Get current inventory status."""
        try:
            query = """
                WITH latest_stock AS (
                    SELECT DISTINCT ON (product_id, warehouse_id)
                        product_id,
                        warehouse_id,
                        current_stock,
                        reorder_point
                    FROM inventory_metrics
                    WHERE time >= NOW() - INTERVAL '1 day'
                    ORDER BY product_id, warehouse_id, time DESC
                )
                SELECT 
                    product_id,
                    warehouse_id,
                    current_stock,
                    reorder_point,
                    CASE 
                        WHEN current_stock <= reorder_point 
                        THEN TRUE 
                        ELSE FALSE 
                    END as needs_restock
                FROM latest_stock
            """
            
            if warehouse_id:
                query += " WHERE warehouse_id = %(warehouse_id)s"
                self.cur.execute(query, {'warehouse_id': warehouse_id})
            else:
                self.cur.execute(query)
            
            return self.cur.fetchall()
            
        except Exception as e:
            logger.error(f"Inventory query error: {e}")
            raise
    
    def _disconnect(self) -> None:
        """Close TimescaleDB connection."""
        if hasattr(self, 'cur'):
            self.cur.close()
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def health_check(self) -> Dict[str, Any]:
        """Check TimescaleDB connection health."""
        try:
            self.cur.execute("SELECT version();")
            version = self.cur.fetchone()['version']
            
            self.cur.execute("""
                SELECT * FROM timescaledb_information.hypertables;
            """)
            hypertables = self.cur.fetchall()
            
            return {
                'status': 'healthy',
                'timestamp': datetime.utcnow(),
                'version': version,
                'hypertables': len(hypertables)
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow()
            } 