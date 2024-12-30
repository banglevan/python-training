"""
Query performance exercises.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from datetime import datetime
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryPerformance:
    """Query performance management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize performance manager."""
        self.spark = spark
        self.stats_cache = {}
        self.plan_cache = {}
    
    def analyze_query(
        self,
        query: str,
        collect_stats: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze query performance.
        
        Args:
            query: SQL query
            collect_stats: Whether to collect statistics
        
        Returns:
            Query analysis
        """
        try:
            # Parse query
            parsed = self.spark.sql(query)
            
            # Get execution plan
            plan = parsed.explain(True)
            
            # Collect statistics if needed
            if collect_stats:
                self._collect_table_stats(parsed)
            
            # Analyze plan
            analysis = self._analyze_plan(plan)
            
            # Cache plan
            self.plan_cache[query] = {
                'plan': plan,
                'analysis': analysis,
                'timestamp': datetime.now().isoformat()
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze query: {e}")
            raise
    
    def optimize_query(
        self,
        query: str
    ) -> Dict[str, Any]:
        """
        Suggest query optimizations.
        
        Args:
            query: SQL query
        
        Returns:
            Optimization suggestions
        """
        try:
            # Analyze query
            analysis = self.analyze_query(query)
            
            # Generate suggestions
            suggestions = []
            
            # Check for full table scans
            if analysis.get('full_table_scans'):
                suggestions.append({
                    'type': 'INDEX',
                    'message': 'Consider adding indexes for: ' + 
                              ', '.join(analysis['full_table_scans'])
                })
            
            # Check for large shuffles
            if analysis.get('large_shuffles'):
                suggestions.append({
                    'type': 'PARTITION',
                    'message': 'Consider partitioning data to reduce shuffling'
                })
            
            # Check for skewed joins
            if analysis.get('skewed_joins'):
                suggestions.append({
                    'type': 'BROADCAST',
                    'message': 'Consider broadcasting small tables: ' +
                              ', '.join(analysis['skewed_joins'])
                })
            
            return {
                'analysis': analysis,
                'suggestions': suggestions
            }
            
        except Exception as e:
            logger.error(f"Failed to optimize query: {e}")
            raise
    
    def collect_table_stats(
        self,
        table_name: str
    ) -> None:
        """
        Collect table statistics.
        
        Args:
            table_name: Table name
        """
        try:
            # Analyze table
            self.spark.sql(f"""
                ANALYZE TABLE {table_name}
                COMPUTE STATISTICS FOR ALL COLUMNS
            """)
            
            # Get statistics
            stats = self.spark.sql(f"""
                DESCRIBE EXTENDED {table_name}
            """).collect()
            
            # Cache statistics
            self.stats_cache[table_name] = {
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Collected statistics for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to collect statistics: {e}")
            raise
    
    def get_cached_plan(
        self,
        query: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached query plan.
        
        Args:
            query: SQL query
        
        Returns:
            Cached plan if available
        """
        return self.plan_cache.get(query)
    
    def clear_cache(
        self,
        cache_type: str = "all"
    ) -> None:
        """
        Clear cache.
        
        Args:
            cache_type: Type of cache to clear
        """
        try:
            if cache_type in ["all", "stats"]:
                self.stats_cache.clear()
            
            if cache_type in ["all", "plan"]:
                self.plan_cache.clear()
            
            logger.info(f"Cleared {cache_type} cache")
            
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            raise
    
    def _collect_table_stats(self, df: Any) -> None:
        """Collect statistics for tables in DataFrame."""
        try:
            # Get referenced tables
            tables = set()
            for attr in df.schema:
                if hasattr(attr, 'metadata'):
                    table = attr.metadata.get('table')
                    if table:
                        tables.add(table)
            
            # Collect statistics
            for table in tables:
                if table not in self.stats_cache:
                    self.collect_table_stats(table)
            
        except Exception as e:
            logger.error(f"Failed to collect table statistics: {e}")
            raise
    
    def _analyze_plan(self, plan: str) -> Dict[str, Any]:
        """Analyze execution plan."""
        try:
            analysis = {
                'full_table_scans': [],
                'large_shuffles': False,
                'skewed_joins': []
            }
            
            # Check for full table scans
            if "Scan" in plan and "Filter" not in plan:
                analysis['full_table_scans'].append(
                    self._extract_table_name(plan)
                )
            
            # Check for large shuffles
            if "Exchange" in plan:
                analysis['large_shuffles'] = True
            
            # Check for skewed joins
            if "SortMergeJoin" in plan:
                tables = self._extract_join_tables(plan)
                for table in tables:
                    if self._is_small_table(table):
                        analysis['skewed_joins'].append(table)
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze plan: {e}")
            raise
    
    def _extract_table_name(self, plan: str) -> str:
        """Extract table name from plan."""
        # Implementation depends on Spark version
        return "table_name"
    
    def _extract_join_tables(self, plan: str) -> List[str]:
        """Extract tables involved in joins."""
        # Implementation depends on Spark version
        return ["table1", "table2"]
    
    def _is_small_table(self, table: str) -> bool:
        """Check if table is small enough for broadcast."""
        try:
            if table in self.stats_cache:
                stats = self.stats_cache[table]['stats']
                # Check if table size is less than broadcast threshold
                return True
            return False
            
        except Exception:
            return False 