"""
MySQL Performance Optimization
----------------------------

TARGET:
1. Query Optimization
   - Reduce execution time
   - Minimize resource usage
   - Improve concurrency

2. Index Strategy
   - Optimal index selection
   - Index maintenance
   - Coverage analysis

3. Buffer Pool Management
   - Memory utilization
   - Cache hit ratio
   - I/O reduction

IMPLEMENTATION APPROACH:
-----------------------

1. Query Analysis:
   - Execution Plan Evaluation
     * Access pattern analysis
     * Join optimization
     * Filter effectiveness
   - Performance Metrics
     * Response time
     * Resource consumption
     * Result set size

2. Index Optimization:
   - Index Selection
     * Column cardinality
     * Query patterns
     * Update frequency
   - Index Types
     * B-tree indexes
     * Covering indexes
     * Composite indexes

3. Buffer Pool Tuning:
   - Size Optimization
     * Working set analysis
     * Memory allocation
     * Page replacement
   - Instance Configuration
     * Multiple instances
     * Page size
     * Flush methods

PERFORMANCE METRICS:
------------------
1. Query Performance
   - Execution time
   - Rows examined/returned ratio
   - Temporary table usage

2. Index Effectiveness
   - Index hit ratio
   - Index scan vs table scan
   - Index maintenance overhead

3. Memory Management
   - Buffer pool hit ratio
   - Page read/write rates
   - Free buffer availability

OPTIMIZATION TECHNIQUES:
----------------------
1. Query Rewrites
   - Subquery optimization
   - JOIN restructuring
   - WHERE clause ordering

2. Index Strategies
   - Selective indexes
   - Index merging
   - Index condition pushdown

3. Configuration Tuning
   - Buffer pool size
   - Read/write ratio
   - Thread concurrency

Example Usage:
-------------
mysql = MySQLPerformance(database='test_db', user='admin')

# Analyze query performance
analysis = mysql.analyze_query(
    "SELECT * FROM orders WHERE status = 'pending'"
)

# Optimize query structure
optimization = mysql.optimize_query(
    query,
    'orders'
)

# Tune buffer pool
tuning = mysql.tune_buffer_pool()
"""

import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, List, Any
import time
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class QueryMetrics:
    """Query performance metrics."""
    execution_time: float
    rows_examined: int
    rows_sent: int
    tmp_tables: int
    filesort: bool

class MySQLPerformance:
    """MySQL performance optimization handler."""
    
    def __init__(
        self,
        database: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: int = 3306
    ):
        """Initialize connection."""
        self.conn_params = {
            'database': database,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
        self.cur = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = mysql.connector.connect(
                **self.conn_params
            )
            self.cur = self.conn.cursor(
                dictionary=True
            )
            logger.info("Database connection established")
        except Error as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def analyze_query(self, query: str):
        """Analyze query performance."""
        try:
            # Get query plan
            self.cur.execute(f"EXPLAIN FORMAT=JSON {query}")
            plan = self.cur.fetchone()
            
            # Execute with profiling
            self.cur.execute("SET profiling = 1")
            
            start_time = time.time()
            self.cur.execute(query)
            results = self.cur.fetchall()
            duration = time.time() - start_time
            
            # Get profile data
            self.cur.execute("""
                SELECT * FROM INFORMATION_SCHEMA.PROFILING
                WHERE QUERY_ID = (
                    SELECT MAX(QUERY_ID) 
                    FROM INFORMATION_SCHEMA.PROFILING
                )
            """)
            profile = self.cur.fetchall()
            
            # Get status info
            self.cur.execute("SHOW SESSION STATUS")
            status = {
                r['Variable_name']: r['Value']
                for r in self.cur.fetchall()
            }
            
            metrics = QueryMetrics(
                execution_time=duration,
                rows_examined=int(
                    status['Rows_examined']
                ),
                rows_sent=int(
                    status['Rows_sent']
                ),
                tmp_tables=int(
                    status['Created_tmp_tables']
                ),
                filesort='filesort' in str(plan)
            )
            
            return {
                'plan': plan,
                'profile': profile,
                'metrics': metrics
            }
            
        except Error as e:
            logger.error(f"Query analysis error: {e}")
            raise
    
    def optimize_query(
        self,
        query: str,
        table_name: str = None
    ):
        """Optimize query performance."""
        try:
            # Analyze current performance
            before_metrics = self.analyze_query(query)
            
            # Get table structure
            if table_name:
                self.cur.execute(f"""
                    SHOW CREATE TABLE {table_name}
                """)
                table_info = self.cur.fetchone()
                
                # Check existing indexes
                self.cur.execute(f"""
                    SHOW INDEX FROM {table_name}
                """)
                indexes = self.cur.fetchall()
                
                # Suggest indexes
                self.cur.execute(f"""
                    ANALYZE TABLE {table_name}
                """)
                
                suggested_indexes = []
                
                # Check for full table scans
                if 'ALL' in str(before_metrics['plan']):
                    cols = self._extract_where_columns(query)
                    for col in cols:
                        if not any(
                            idx['Column_name'] == col
                            for idx in indexes
                        ):
                            suggested_indexes.append(col)
                
                # Check for sorting
                if before_metrics['metrics'].filesort:
                    cols = self._extract_order_columns(query)
                    for col in cols:
                        if not any(
                            idx['Column_name'] == col
                            for idx in indexes
                        ):
                            suggested_indexes.append(col)
            
            # Suggest query rewrites
            rewrites = []
            
            # Check for SELECT *
            if 'SELECT *' in query.upper():
                rewrites.append(
                    "Replace SELECT * with specific columns"
                )
            
            # Check for subqueries
            if 'SELECT' in query.upper().split('FROM')[1]:
                rewrites.append(
                    "Consider replacing subquery with JOIN"
                )
            
            return {
                'current_metrics': before_metrics['metrics'],
                'suggested_indexes': suggested_indexes,
                'query_rewrites': rewrites
            }
            
        except Error as e:
            logger.error(f"Query optimization error: {e}")
            raise
    
    def tune_buffer_pool(self):
        """Tune InnoDB buffer pool."""
        try:
            # Get current settings
            self.cur.execute("""
                SHOW VARIABLES 
                WHERE Variable_name LIKE 'innodb_buffer_pool%'
            """)
            current_settings = {
                r['Variable_name']: r['Value']
                for r in self.cur.fetchall()
            }
            
            # Get buffer pool stats
            self.cur.execute("""
                SHOW ENGINE INNODB STATUS
            """)
            status = self.cur.fetchone()['Status']
            
            # Parse buffer pool info
            pool_info = {}
            for line in status.split('\n'):
                if 'Buffer pool size' in line:
                    pool_info['size'] = line.split()[-1]
                elif 'Free buffers' in line:
                    pool_info['free'] = line.split()[-1]
                elif 'Database pages' in line:
                    pool_info['used'] = line.split()[-1]
            
            # Calculate recommendations
            total_memory = 16 * 1024  # Example: 16GB
            recommended = {
                'innodb_buffer_pool_size': 
                    min(total_memory * 0.7, 
                        int(pool_info['size']) * 1.2),
                'innodb_buffer_pool_instances':
                    max(1, total_memory // 1024)
            }
            
            return {
                'current_settings': current_settings,
                'pool_stats': pool_info,
                'recommendations': recommended
            }
            
        except Error as e:
            logger.error(f"Buffer pool tuning error: {e}")
            raise
    
    def _extract_where_columns(self, query: str):
        """Extract columns from WHERE clause."""
        try:
            where_part = query.upper().split('WHERE')[1]
            where_part = where_part.split('ORDER')[0]
            
            cols = []
            parts = where_part.split('AND')
            
            for part in parts:
                if '=' in part:
                    col = part.split('=')[0].strip()
                    cols.append(col.lower())
            
            return cols
            
        except Exception:
            return []
    
    def _extract_order_columns(self, query: str):
        """Extract columns from ORDER BY clause."""
        try:
            order_part = query.upper().split('ORDER BY')[1]
            order_part = order_part.split('LIMIT')[0]
            
            return [
                col.strip().lower()
                for col in order_part.split(',')
            ]
            
        except Exception:
            return []
    
    def disconnect(self):
        """Close database connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main function."""
    mysql = MySQLPerformance(
        database='test_db',
        user='test_user',
        password='test_pass'
    )
    
    try:
        # Connect to database
        mysql.connect()
        
        # Example: Analyze query
        query = """
            SELECT c.*, o.order_date
            FROM customers c
            LEFT JOIN orders o ON c.id = o.customer_id
            WHERE c.status = 'active'
            ORDER BY o.order_date DESC
        """
        
        analysis = mysql.analyze_query(query)
        logger.info(f"Query analysis: {analysis}")
        
        # Example: Optimize query
        optimization = mysql.optimize_query(
            query,
            'customers'
        )
        logger.info(f"Query optimization: {optimization}")
        
        # Example: Tune buffer pool
        tuning = mysql.tune_buffer_pool()
        logger.info(f"Buffer pool tuning: {tuning}")
        
    finally:
        mysql.disconnect()

if __name__ == '__main__':
    main() 