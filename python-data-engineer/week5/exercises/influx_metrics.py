"""
InfluxDB Time Series Metrics
---------------------------

TARGET:
1. Data Ingestion
   - Write optimization
   - Batch processing
   - Line protocol

2. Continuous Queries
   - Downsampling
   - Window functions
   - Aggregation rules

3. Retention Policies
   - Data lifecycle
   - Storage optimization
   - Shard management

IMPLEMENTATION APPROACH:
-----------------------

1. Data Management:
   - Write Strategies
     * Batch size optimization
     * Tag cardinality
     * Field selection
   - Read Patterns
     * Time range queries
     * Group by time
     * Series filtering
   - Schema Design
     * Measurement structure
     * Tag vs Field choice
     * Indexing impact

2. Query Optimization:
   - Continuous Queries
     * Automatic downsampling
     * Pre-aggregation
     * Resource usage
   - Window Functions
     * Moving averages
     * Sliding windows
     * Custom periods
   - Query Patterns
     * Time bucket alignment
     * Series cardinality
     * Memory constraints

3. Storage Management:
   - Retention Rules
     * Duration policies
     * Replication factors
     * Shard duration
   - Compaction
     * TSM engine
     * Compression ratio
     * Write amplification
   - Monitoring
     * Series growth
     * Disk usage
     * Query performance

PERFORMANCE METRICS:
------------------
1. Write Performance
   - Points per second
   - Batch efficiency
   - Write failures

2. Query Efficiency
   - Response time
   - Memory usage
   - Series scanned

3. Storage Impact
   - Compression ratio
   - Shard metrics
   - Disk usage

Example Usage:
-------------
influx = InfluxMetrics('http://localhost:8086')

# Write batch metrics
influx.write_batch(
    'system_metrics',
    points=[
        {'cpu_usage': 75.2, 'host': 'server1'},
        {'cpu_usage': 82.1, 'host': 'server2'}
    ],
    tags={'env': 'prod'}
)

# Create continuous query
influx.create_continuous_query(
    'cpu_hourly',
    'SELECT mean(cpu_usage) FROM system_metrics GROUP BY time(1h)'
)
"""

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from typing import Dict, List, Any
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InfluxMetrics:
    """InfluxDB metrics handler."""
    
    def __init__(
        self,
        url: str = 'http://localhost:8086',
        token: str = None,
        org: str = None,
        bucket: str = 'metrics'
    ):
        """Initialize InfluxDB connection."""
        self.client = InfluxDBClient(
            url=url,
            token=token,
            org=org
        )
        self.bucket = bucket
        self.write_api = self.client.write_api(
            write_options=SYNCHRONOUS
        )
        self.query_api = self.client.query_api()
        
        logger.info(f"Connected to InfluxDB: {url}")
    
    def write_batch(
        self,
        measurement: str,
        points: List[Dict],
        tags: Dict = None,
        batch_size: int = 1000
    ):
        """Write batch of metrics."""
        try:
            start_time = time.time()
            
            # Prepare points
            influx_points = []
            for data in points:
                point = Point(measurement)
                
                # Add fields
                for key, value in data.items():
                    if isinstance(value, (int, float)):
                        point.field(key, value)
                    else:
                        point.tag(key, str(value))
                
                # Add common tags
                if tags:
                    for key, value in tags.items():
                        point.tag(key, value)
                
                influx_points.append(point)
                
                # Write batch if size reached
                if len(influx_points) >= batch_size:
                    self._write_points(influx_points)
                    influx_points = []
            
            # Write remaining points
            if influx_points:
                self._write_points(influx_points)
            
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'write_batch',
                'measurement': measurement,
                'points': len(points),
                'duration': f"{duration:.6f}s",
                'rate': len(points) / duration
            }
            
            logger.info(f"Write metrics: {metrics}")
            
        except Exception as e:
            logger.error(f"Write error: {e}")
            raise
    
    def _write_points(self, points: List[Point]):
        """Write points to InfluxDB."""
        self.write_api.write(
            bucket=self.bucket,
            record=points
        )
    
    def create_continuous_query(
        self,
        name: str,
        query: str,
        every: str = '1h',
        for_each: List[str] = None
    ):
        """Create continuous query."""
        try:
            # Build query
            cq_query = f"""
                CREATE CONTINUOUS QUERY {name}
                ON {self.bucket}
                BEGIN
                    {query}
                END
            """
            
            if every:
                cq_query = cq_query.replace(
                    'BEGIN',
                    f'RESAMPLE EVERY {every} BEGIN'
                )
            
            if for_each:
                cq_query += f" GROUP BY {','.join(for_each)}"
            
            # Execute query
            self.client.query_api().query(cq_query)
            
            logger.info(f"Created CQ: {name}")
            
        except Exception as e:
            logger.error(f"CQ creation error: {e}")
            raise
    
    def create_retention_policy(
        self,
        name: str,
        duration: str,
        replication: int = 1,
        default: bool = False
    ):
        """Create retention policy."""
        try:
            query = f"""
                CREATE RETENTION POLICY {name}
                ON {self.bucket}
                DURATION {duration}
                REPLICATION {replication}
                {'DEFAULT' if default else ''}
            """
            
            self.client.query_api().query(query)
            
            logger.info(
                f"Created RP: {name} ({duration})"
            )
            
        except Exception as e:
            logger.error(f"RP creation error: {e}")
            raise
    
    def query_metrics(
        self,
        query: str,
        params: Dict = None
    ):
        """Query metrics with parameters."""
        try:
            start_time = time.time()
            
            # Execute query
            result = self.query_api.query(
                query,
                params=params
            )
            
            duration = time.time() - start_time
            
            metrics = {
                'operation': 'query',
                'duration': f"{duration:.6f}s"
            }
            
            logger.info(f"Query metrics: {metrics}")
            return result
            
        except Exception as e:
            logger.error(f"Query error: {e}")
            raise
    
    def monitor_stats(self):
        """Monitor InfluxDB statistics."""
        try:
            stats = {
                'database': self.client.query_api().query(
                    'SHOW STATS'
                ),
                'diagnostics': self.client.query_api().query(
                    'SHOW DIAGNOSTICS'
                ),
                'measurements': self.client.query_api().query(
                    f'SHOW MEASUREMENTS ON {self.bucket}'
                )
            }
            
            logger.info("Collected InfluxDB stats")
            return stats
            
        except Exception as e:
            logger.error(f"Stats error: {e}")
            raise
    
    def close(self):
        """Close InfluxDB connection."""
        if self.client:
            self.client.close()
            logger.info("InfluxDB connection closed")

def main():
    """Main function."""
    influx = InfluxMetrics()
    
    try:
        # Example: Write metrics
        points = [
            {
                'cpu_usage': 75.2,
                'memory_used': 8.5,
                'host': 'server1'
            },
            {
                'cpu_usage': 82.1,
                'memory_used': 7.8,
                'host': 'server2'
            }
        ]
        
        influx.write_batch(
            'system_metrics',
            points,
            tags={'env': 'prod'}
        )
        
        # Example: Create continuous query
        influx.create_continuous_query(
            'cpu_hourly',
            """
            SELECT mean(cpu_usage) as cpu_avg,
                   max(cpu_usage) as cpu_max
            FROM system_metrics
            GROUP BY time(1h)
            """
        )
        
        # Example: Create retention policy
        influx.create_retention_policy(
            'short_term',
            '7d',
            replication=1
        )
        
        # Example: Query metrics
        results = influx.query_metrics(
            """
            SELECT mean(cpu_usage)
            FROM system_metrics
            WHERE time > now() - 1h
            GROUP BY time(5m)
            """
        )
        
    finally:
        influx.close()

if __name__ == '__main__':
    main() 