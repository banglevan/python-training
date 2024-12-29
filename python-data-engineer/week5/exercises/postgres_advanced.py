"""
Advanced PostgreSQL Operations & Optimization
------------------------------------------

TARGET:
1. Table Partitioning
   - Improve query performance for large tables
   - Manage data lifecycle efficiently
   - Reduce maintenance overhead

2. Database Replication
   - Ensure high availability
   - Scale read operations
   - Implement disaster recovery

3. VACUUM & Maintenance
   - Reclaim disk space
   - Prevent transaction ID wraparound
   - Optimize table statistics

IMPLEMENTATION APPROACH:
-----------------------

1. Partitioning Strategy:
   - Range Partitioning: Time-based data segmentation
     * Quarterly partitions for historical data
     * Monthly partitions for recent data
   - List Partitioning: Category-based segmentation
     * Separate partitions by business unit
     * Custom partition bounds

2. Replication Architecture:
   - Streaming Replication
     * Synchronous: Ensure data consistency
     * Asynchronous: Optimize performance
   - Logical Replication
     * Selective table replication
     * Cross-version compatibility

3. Maintenance Operations:
   - Automated VACUUM
     * Dead tuple threshold monitoring
     * Bloat prevention
   - Manual VACUUM
     * Full table rebuild
     * Index cleanup

PERFORMANCE CONSIDERATIONS:
-------------------------
1. Partition Pruning
   - Query planner optimization
   - Partition elimination
   - Access pattern analysis

2. Replication Lag
   - Monitor write-ahead log (WAL)
   - Network bandwidth impact
   - Catchup time measurement

3. Resource Usage
   - Disk I/O patterns
   - Memory allocation
   - CPU utilization

BENCHMARKS & METRICS:
--------------------
1. Query Performance
   - Response time distribution
   - Throughput measurements
   - Resource utilization

2. Maintenance Impact
   - Space reclamation
   - Operation duration
   - System load

3. Replication Health
   - Lag time tracking
   - Data consistency checks
   - Failover testing

Example Usage:
-------------
pg = PostgresAdvanced(dbname='test_db', user='admin')

# Setup time-based partitioning
pg.setup_partitioning(
    'events',
    'created_at',
    'range',
    ranges=[
        {'suffix': '2024_q1', 'start': '2024-01-01'},
        {'suffix': '2024_q2', 'start': '2024-04-01'}
    ]
)

# Configure streaming replication
pg.setup_replication(
    'streaming',
    master_config={'slot_name': 'replica_1'},
    slave_config={'conninfo': 'host=slave'}
)

# Perform maintenance
pg.perform_vacuum('large_table', vacuum_type='FULL')
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresAdvanced:
    """Advanced PostgreSQL operations handler."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: int = 5432
    ):
        """Initialize connection."""
        self.conn_params = {
            'dbname': dbname,
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
            self.conn = psycopg2.connect(**self.conn_params)
            self.cur = self.conn.cursor(
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def setup_partitioning(
        self,
        table_name: str,
        partition_key: str,
        partition_type: str,
        ranges: List[Dict] = None,
        list_values: List[Any] = None
    ):
        """Set up table partitioning."""
        try:
            # Create partitioned table
            if partition_type == 'range':
                create_stmt = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL,
                        created_at TIMESTAMP,
                        data JSONB
                    ) PARTITION BY RANGE ({partition_key})
                """
                
                self.cur.execute(create_stmt)
                
                # Create partitions
                for r in ranges:
                    partition_name = f"{table_name}_{r['suffix']}"
                    self.cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {partition_name}
                        PARTITION OF {table_name}
                        FOR VALUES FROM ('{r['start']}')
                        TO ('{r['end']}')
                    """)
                    
            elif partition_type == 'list':
                create_stmt = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id SERIAL,
                        category VARCHAR(50),
                        data JSONB
                    ) PARTITION BY LIST ({partition_key})
                """
                
                self.cur.execute(create_stmt)
                
                # Create partitions
                for value in list_values:
                    partition_name = f"{table_name}_{value.lower()}"
                    self.cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {partition_name}
                        PARTITION OF {table_name}
                        FOR VALUES IN ('{value}')
                    """)
            
            self.conn.commit()
            logger.info(
                f"Partitioning set up for {table_name}"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Partitioning error: {e}")
            raise
    
    def setup_replication(
        self,
        replication_type: str,
        master_config: Dict = None,
        slave_config: Dict = None
    ):
        """Set up database replication."""
        try:
            if replication_type == 'streaming':
                # Configure master
                self.cur.execute("""
                    ALTER SYSTEM SET 
                    wal_level = replica
                """)
                
                self.cur.execute("""
                    ALTER SYSTEM SET 
                    max_wal_senders = 10
                """)
                
                self.cur.execute("""
                    ALTER SYSTEM SET
                    max_replication_slots = 10
                """)
                
                # Create replication slot
                if master_config.get('create_slot'):
                    self.cur.execute(f"""
                        SELECT pg_create_physical_replication_slot(
                            '{master_config['slot_name']}'
                        )
                    """)
                
                # Configure slave
                if slave_config:
                    recovery_conf = [
                        "standby_mode = 'on'",
                        f"primary_conninfo = '{slave_config['conninfo']}'",
                        f"primary_slot_name = '{master_config['slot_name']}'"
                    ]
                    
                    with open('recovery.conf', 'w') as f:
                        f.write('\n'.join(recovery_conf))
                
            elif replication_type == 'logical':
                # Create publication
                self.cur.execute(f"""
                    CREATE PUBLICATION {master_config['pub_name']}
                    FOR TABLE {', '.join(master_config['tables'])}
                """)
                
                # Create subscription
                if slave_config:
                    self.cur.execute(f"""
                        CREATE SUBSCRIPTION {slave_config['sub_name']}
                        CONNECTION '{slave_config['conninfo']}'
                        PUBLICATION {master_config['pub_name']}
                    """)
            
            self.conn.commit()
            logger.info(
                f"{replication_type} replication configured"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Replication setup error: {e}")
            raise
    
    def perform_vacuum(
        self,
        table_name: str,
        vacuum_type: str = 'FULL',
        analyze: bool = True
    ):
        """Perform VACUUM operation."""
        try:
            # Get table stats before
            self.cur.execute(f"""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('{table_name}')) as size,
                    n_dead_tup as dead_tuples,
                    n_live_tup as live_tuples
                FROM pg_stat_user_tables
                WHERE relname = '{table_name}'
            """)
            before_stats = self.cur.fetchone()
            
            # Perform VACUUM
            start_time = time.time()
            
            vacuum_stmt = f"VACUUM"
            if vacuum_type == 'FULL':
                vacuum_stmt += " FULL"
            if analyze:
                vacuum_stmt += " ANALYZE"
            vacuum_stmt += f" {table_name}"
            
            self.cur.execute(vacuum_stmt)
            
            duration = time.time() - start_time
            
            # Get table stats after
            self.cur.execute(f"""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('{table_name}')) as size,
                    n_dead_tup as dead_tuples,
                    n_live_tup as live_tuples
                FROM pg_stat_user_tables
                WHERE relname = '{table_name}'
            """)
            after_stats = self.cur.fetchone()
            
            # Calculate improvements
            results = {
                'table': table_name,
                'vacuum_type': vacuum_type,
                'duration': f"{duration:.2f}s",
                'before': before_stats,
                'after': after_stats
            }
            
            logger.info(
                f"VACUUM completed: {results}"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"VACUUM error: {e}")
            raise
    
    def monitor_maintenance(self):
        """Monitor database maintenance."""
        try:
            # Check autovacuum status
            self.cur.execute("""
                SELECT name, setting, unit
                FROM pg_settings
                WHERE name LIKE 'autovacuum%'
            """)
            autovacuum_settings = self.cur.fetchall()
            
            # Check table statistics
            self.cur.execute("""
                SELECT 
                    relname as table_name,
                    n_live_tup as live_tuples,
                    n_dead_tup as dead_tuples,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                ORDER BY n_dead_tup DESC
            """)
            table_stats = self.cur.fetchall()
            
            # Check bloat
            self.cur.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(
                        schemaname || '.' || tablename
                    )) as total_size,
                    pg_size_pretty(pg_table_size(
                        schemaname || '.' || tablename
                    )) as table_size,
                    pg_size_pretty(pg_indexes_size(
                        schemaname || '.' || tablename
                    )) as index_size
                FROM pg_tables
                WHERE schemaname NOT IN (
                    'pg_catalog',
                    'information_schema'
                )
                ORDER BY pg_total_relation_size(
                    schemaname || '.' || tablename
                ) DESC
            """)
            bloat_stats = self.cur.fetchall()
            
            return {
                'autovacuum_settings': autovacuum_settings,
                'table_stats': table_stats,
                'bloat_stats': bloat_stats
            }
            
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main function."""
    pg = PostgresAdvanced(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )
    
    try:
        # Connect to database
        pg.connect()
        
        # Example: Set up range partitioning
        pg.setup_partitioning(
            'events',
            'created_at',
            'range',
            ranges=[
                {
                    'suffix': '2024_q1',
                    'start': '2024-01-01',
                    'end': '2024-04-01'
                },
                {
                    'suffix': '2024_q2',
                    'start': '2024-04-01',
                    'end': '2024-07-01'
                }
            ]
        )
        
        # Example: Set up streaming replication
        pg.setup_replication(
            'streaming',
            master_config={
                'slot_name': 'replica_1',
                'create_slot': True
            },
            slave_config={
                'conninfo': 'host=slave port=5432 user=repl password=repl'
            }
        )
        
        # Example: Perform VACUUM
        results = pg.perform_vacuum(
            'large_table',
            vacuum_type='FULL',
            analyze=True
        )
        
        # Example: Monitor maintenance
        stats = pg.monitor_maintenance()
        logger.info(f"Maintenance stats: {stats}")
        
    finally:
        pg.disconnect()

if __name__ == '__main__':
    main() 