"""
TimescaleDB Time Series Analytics
-------------------------------

TARGET:
1. Hypertable Management
   - Table creation
   - Chunk management
   - Compression policies

2. Continuous Aggregates
   - Real-time materialization
   - Refresh policies
   - Query optimization

3. Data Lifecycle
   - Retention policies
   - Data tiering
   - Automated maintenance

IMPLEMENTATION APPROACH:
-----------------------

1. Data Architecture:
   - Hypertable Design
     * Partitioning strategy
     * Chunk intervals
     * Dimension modeling
   - Schema Optimization
     * Column selection
     * Index strategy
     * Compression settings
   - Access Patterns
     * Time-based queries
     * Cross-chunk operations
     * Parallel execution

2. Aggregation Strategy:
   - Continuous Aggregates
     * Materialization intervals
     * Refresh scheduling
     * View maintenance
   - Window Functions
     * Moving aggregates
     * Time buckets
     * Custom periods
   - Query Planning
     * Chunk exclusion
     * Parallel scanning
     * Resource usage

3. Lifecycle Management:
   - Retention Rules
     * Age-based retention
     * Custom policies
     * Multi-node setup
   - Data Tiering
     * Hot/warm/cold data
     * Compression ratios
     * Storage optimization
   - Maintenance
     * Vacuum operations
     * Stats collection
     * Performance monitoring

PERFORMANCE METRICS:
------------------
1. Query Performance
   - Response time
   - Chunk scanning
   - Cache hits

2. Storage Efficiency
   - Compression ratio
   - Chunk distribution
   - Space usage

3. Maintenance Impact
   - Background workers
   - IO patterns
   - CPU utilization

Example Usage:
-------------
tsdb = TimescaleAnalytics(
    dbname='metrics_db',
    user='admin'
)

# Create hypertable
tsdb.create_hypertable(
    'sensor_data',
    'timestamp',
    chunk_interval='1 day'
)

# Setup continuous aggregate
tsdb.create_continuous_aggregate(
    'hourly_metrics',
    'SELECT time_bucket(\'1 hour\', timestamp) as hour,
     avg(temperature) as temp_avg
     FROM sensor_data GROUP BY 1'
)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimescaleAnalytics:
    """TimescaleDB analytics handler."""
    
    def __init__(
        self,
        dbname: str = 'postgres',
        user: str = 'postgres',
        password: str = None,
        host: str = 'localhost',
        port: int = 5432
    ):
        """Initialize TimescaleDB connection."""
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor(
            cursor_factory=RealDictCursor
        )
        logger.info(f"Connected to TimescaleDB: {dbname}")
    
    def create_hypertable(
        self,
        table: str,
        time_column: str,
        chunk_interval: str = '1 day',
        dimensions: List[str] = None
    ):
        """Create hypertable from regular table."""
        try:
            # Create hypertable
            self.cur.execute(f"""
                SELECT create_hypertable(
                    '{table}',
                    '{time_column}',
                    chunk_time_interval => interval '{chunk_interval}'
                    {', partitioning_column => \'' + dimensions[0] + '\''
                     if dimensions else ''}
                )
            """)
            
            # Set chunk compression
            self.cur.execute(f"""
                ALTER TABLE {table} 
                SET (
                    timescaledb.compress,
                    timescaledb.compress_orderby = '{time_column}'
                )
            """)
            
            self.conn.commit()
            logger.info(f"Created hypertable: {table}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Hypertable creation error: {e}")
            raise
    
    def create_continuous_aggregate(
        self,
        view_name: str,
        query: str,
        refresh_interval: str = '1 hour',
        refresh_lag: str = '1 hour',
        materialized_only: bool = True
    ):
        """Create continuous aggregate view."""
        try:
            # Create view
            self.cur.execute(f"""
                CREATE MATERIALIZED VIEW {view_name}
                WITH (timescaledb.continuous) AS
                {query}
                WITH NO DATA
            """)
            
            # Set refresh policy
            self.cur.execute(f"""
                SELECT add_continuous_aggregate_policy(
                    '{view_name}',
                    start_offset => interval '{refresh_lag}',
                    end_offset => interval '1 minute',
                    schedule_interval => interval '{refresh_interval}'
                )
            """)
            
            # Initial refresh
            self.cur.execute(f"""
                CALL refresh_continuous_aggregate(
                    '{view_name}',
                    NULL,
                    NULL
                )
            """)
            
            self.conn.commit()
            logger.info(f"Created continuous aggregate: {view_name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Continuous aggregate error: {e}")
            raise
    
    def set_retention_policy(
        self,
        table: str,
        interval: str,
        cascade: bool = True
    ):
        """Set data retention policy."""
        try:
            self.cur.execute(f"""
                SELECT add_retention_policy(
                    '{table}',
                    interval '{interval}',
                    cascade => {cascade}
                )
            """)
            
            self.conn.commit()
            logger.info(
                f"Set retention policy: {table} ({interval})"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Retention policy error: {e}")
            raise
    
    def compress_chunks(
        self,
        table: str,
        older_than: str = '7 days'
    ):
        """Compress chunks older than specified interval."""
        try:
            self.cur.execute(f"""
                SELECT compress_chunk(chunk)
                FROM timescaledb_information.chunks
                WHERE hypertable_name = '{table}'
                AND range_start < now() - interval '{older_than}'
            """)
            
            self.conn.commit()
            logger.info(f"Compressed chunks: {table}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Compression error: {e}")
            raise
    
    def analyze_query(
        self,
        query: str,
        params: tuple = None
    ):
        """Analyze query execution plan."""
        try:
            start_time = time.time()
            
            # Get query plan
            self.cur.execute(
                f"EXPLAIN (ANALYZE, BUFFERS) {query}",
                params
            )
            plan = self.cur.fetchall()
            
            # Execute query
            self.cur.execute(query, params)
            results = self.cur.fetchall()
            
            duration = time.time() - start_time
            
            analysis = {
                'plan': plan,
                'results': len(results),
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Query analysis: {analysis}")
            return analysis
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Query analysis error: {e}")
            raise
    
    def monitor_stats(self):
        """Monitor TimescaleDB statistics."""
        try:
            stats = {
                'chunks': self.cur.execute("""
                    SELECT * FROM chunks_detailed_size
                """).fetchall(),
                'compression': self.cur.execute("""
                    SELECT * FROM compression_stats
                """).fetchall(),
                'retention': self.cur.execute("""
                    SELECT * FROM timescaledb_information.jobs
                    WHERE application_name LIKE 'Retention Policy%'
                """).fetchall()
            }
            
            logger.info("Collected TimescaleDB stats")
            return stats
            
        except Exception as e:
            logger.error(f"Stats monitoring error: {e}")
            raise
    
    def close(self):
        """Close TimescaleDB connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("TimescaleDB connection closed")

def main():
    """Main function."""
    tsdb = TimescaleAnalytics()
    
    try:
        # Example: Create hypertable
        tsdb.create_hypertable(
            'sensor_data',
            'timestamp',
            chunk_interval='1 day',
            dimensions=['sensor_id']
        )
        
        # Example: Create continuous aggregate
        tsdb.create_continuous_aggregate(
            'hourly_metrics',
            """
            SELECT time_bucket('1 hour', timestamp) as hour,
                   sensor_id,
                   avg(temperature) as temp_avg,
                   max(temperature) as temp_max,
                   min(temperature) as temp_min
            FROM sensor_data
            GROUP BY 1, 2
            """
        )
        
        # Example: Set retention policy
        tsdb.set_retention_policy(
            'sensor_data',
            '30 days'
        )
        
        # Example: Compress old chunks
        tsdb.compress_chunks(
            'sensor_data',
            '7 days'
        )
        
        # Example: Analyze query
        analysis = tsdb.analyze_query(
            """
            SELECT hour, avg(temp_avg)
            FROM hourly_metrics
            WHERE hour >= %s
            GROUP BY 1
            ORDER BY 1
            """,
            (datetime.now() - timedelta(days=7),)
        )
        
    finally:
        tsdb.close()

if __name__ == '__main__':
    main() 