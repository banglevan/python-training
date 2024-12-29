"""
Cassandra Data Modeling & Operations
----------------------------------

TARGET:
1. Data Modeling
   - Denormalization patterns
   - Query-driven design
   - Time series optimization

2. Partition Strategy
   - Key distribution
   - Hotspot prevention
   - Size management

3. Consistency Levels
   - Read/Write tuning
   - Availability balance
   - Latency impact

IMPLEMENTATION APPROACH:
-----------------------

1. Data Model Design:
   - Table Structure
     * Primary key composition
     * Clustering columns
     * Secondary indexes
   - Access Patterns
     * Read efficiency
     * Write distribution
     * Time-based queries
   - Denormalization
     * Data duplication
     * Join elimination
     * Update propagation

2. Partition Management:
   - Key Selection
     * Cardinality analysis
     * Distribution balance
     * Growth patterns
   - Size Control
     * Row limitation
     * TTL usage
     * Tombstone handling
   - Anti-patterns
     * Hotspot avoidance
     * Queue anti-pattern
     * Time partitioning

3. Consistency Configuration:
   - Read Levels
     * ONE/QUORUM/ALL
     * LOCAL_QUORUM
     * EACH_QUORUM
   - Write Levels
     * ANY/ONE/QUORUM
     * Batch consistency
     * Lightweight transactions
   - Tuning Factors
     * Latency requirements
     * Consistency needs
     * Network topology

PERFORMANCE METRICS:
------------------
1. Query Performance
   - Response time
   - Partition scan
   - Token range

2. Write Efficiency
   - Throughput
   - Batch size
   - Timeout rates

3. Consistency Impact
   - Read repair
   - Write latency
   - Conflict resolution

Example Usage:
-------------
cassandra = CassandraModel(
    contact_points=['localhost'],
    keyspace='test_ks'
)

# Create table with time-based partitioning
cassandra.create_table(
    'events',
    partition_keys=['event_date', 'category'],
    clustering_keys=['event_time', 'event_id']
)

# Insert with consistency level
cassandra.insert_data(
    'events',
    data={'event_id': 1, 'category': 'test'},
    consistency='QUORUM'
)
"""

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement
import logging
from typing import Dict, List, Any
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CassandraModel:
    """Cassandra data modeling handler."""
    
    def __init__(
        self,
        contact_points: List[str] = ['localhost'],
        port: int = 9042,
        keyspace: str = 'test_ks'
    ):
        """Initialize Cassandra connection."""
        self.cluster = Cluster(
            contact_points=contact_points,
            port=port
        )
        self.session = self.cluster.connect()
        
        # Create keyspace if not exists
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
        """)
        
        self.session.set_keyspace(keyspace)
        logger.info(f"Connected to Cassandra: {keyspace}")
    
    def create_table(
        self,
        table_name: str,
        partition_keys: List[str],
        clustering_keys: List[str] = None,
        columns: Dict[str, str] = None,
        options: Dict[str, Any] = None
    ):
        """Create table with optimized structure."""
        try:
            # Define columns
            if columns is None:
                columns = {}
            
            # Add default timestamp column
            if 'created_at' not in columns:
                columns['created_at'] = 'timestamp'
            
            # Build column definitions
            column_defs = [
                f"{name} {dtype}"
                for name, dtype in columns.items()
            ]
            
            # Build primary key
            pk_parts = []
            if len(partition_keys) > 1:
                pk_parts.append(
                    f"({', '.join(partition_keys)})"
                )
            else:
                pk_parts.append(partition_keys[0])
            
            if clustering_keys:
                pk_parts.extend(clustering_keys)
            
            # Build table options
            table_options = []
            if options:
                for opt, value in options.items():
                    table_options.append(
                        f"{opt} = {value}"
                    )
            
            # Create table
            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(
                        partition_keys +
                        (clustering_keys or []) +
                        [f"{k} {v}" for k, v in columns.items()]
                    )},
                    PRIMARY KEY ({', '.join(pk_parts)})
                )
                {
                    'WITH ' + ' AND '.join(table_options)
                    if table_options else ''
                }
            """
            
            self.session.execute(create_stmt)
            
            logger.info(
                f"Created table: {table_name}"
            )
            
        except Exception as e:
            logger.error(f"Table creation error: {e}")
            raise
    
    def insert_data(
        self,
        table: str,
        data: Dict,
        consistency: str = 'ONE',
        ttl: int = None
    ):
        """Insert data with consistency level."""
        try:
            # Prepare statement
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            
            insert_stmt = f"""
                INSERT INTO {table}
                ({columns})
                VALUES ({placeholders})
                {f'USING TTL {ttl}' if ttl else ''}
            """
            
            # Set consistency level
            stmt = SimpleStatement(
                insert_stmt,
                consistency_level=getattr(
                    ConsistencyLevel,
                    consistency
                )
            )
            
            # Execute
            start_time = time.time()
            self.session.execute(
                stmt,
                list(data.values())
            )
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'insert',
                'table': table,
                'consistency': consistency,
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Insert metrics: {metrics}")
            
        except Exception as e:
            logger.error(f"Insert error: {e}")
            raise
    
    def batch_insert(
        self,
        table: str,
        items: List[Dict],
        consistency: str = 'QUORUM'
    ):
        """Execute batch insert."""
        try:
            batch = BatchStatement(
                consistency_level=getattr(
                    ConsistencyLevel,
                    consistency
                )
            )
            
            # Prepare statement
            columns = ', '.join(items[0].keys())
            placeholders = ', '.join(['%s'] * len(items[0]))
            
            insert_stmt = f"""
                INSERT INTO {table}
                ({columns})
                VALUES ({placeholders})
            """
            
            # Add to batch
            prepared = self.session.prepare(insert_stmt)
            
            for item in items:
                batch.add(
                    prepared,
                    list(item.values())
                )
            
            # Execute batch
            start_time = time.time()
            self.session.execute(batch)
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'batch_insert',
                'table': table,
                'items': len(items),
                'consistency': consistency,
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Batch metrics: {metrics}")
            
        except Exception as e:
            logger.error(f"Batch error: {e}")
            raise
    
    def execute_query(
        self,
        query: str,
        params: List = None,
        consistency: str = 'ONE',
        fetch_size: int = 100
    ):
        """Execute query with metrics."""
        try:
            # Prepare statement
            stmt = SimpleStatement(
                query,
                consistency_level=getattr(
                    ConsistencyLevel,
                    consistency
                ),
                fetch_size=fetch_size
            )
            
            # Execute
            start_time = time.time()
            rows = self.session.execute(
                stmt,
                params or []
            )
            
            # Materialize results
            results = list(rows)
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'query',
                'consistency': consistency,
                'rows': len(results),
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Query metrics: {metrics}")
            return results
            
        except Exception as e:
            logger.error(f"Query error: {e}")
            raise
    
    def close(self):
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

def main():
    """Main function."""
    cassandra = CassandraModel()
    
    try:
        # Example: Create time-series table
        cassandra.create_table(
            'events',
            partition_keys=['event_date', 'category'],
            clustering_keys=['event_time', 'event_id'],
            columns={
                'event_id': 'uuid',
                'category': 'text',
                'event_date': 'date',
                'event_time': 'timestamp',
                'data': 'text'
            },
            options={
                'clustering order by': '(event_time DESC)',
                'compaction': {
                    'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_size': 1,
                    'compaction_window_unit': 'DAYS'
                }
            }
        )
        
        # Example: Insert with QUORUM consistency
        cassandra.insert_data(
            'events',
            {
                'event_id': 'uuid()',
                'category': 'test',
                'event_date': 'toDate(now())',
                'event_time': 'now()',
                'data': '{"test": true}'
            },
            consistency='QUORUM'
        )
        
        # Example: Query with consistency
        results = cassandra.execute_query(
            """
            SELECT * FROM events
            WHERE event_date = toDate(now())
            AND category = 'test'
            """,
            consistency='LOCAL_QUORUM'
        )
        
    finally:
        cassandra.close()

if __name__ == '__main__':
    main() 