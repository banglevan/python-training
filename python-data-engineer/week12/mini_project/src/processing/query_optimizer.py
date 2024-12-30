"""
Query optimization management.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Query optimization management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize optimizer."""
        self.spark = spark
        self.views = {}
        self.stats_cache = {}
    
    def create_materialized_view(
        self,
        name: str,
        query: str,
        refresh_interval: timedelta = timedelta(hours=1),
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Create materialized view.
        
        Args:
            name: View name
            query: SQL query
            refresh_interval: Refresh interval
            partition_by: Partition columns
        """
        try:
            # Create view
            df = self.spark.sql(query)
            
            if partition_by:
                df = df.repartition(*partition_by)
            
            # Save as Delta table
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(name)
            
            # Store metadata
            self.views[name] = {
                'query': query,
                'refresh_interval': refresh_interval,
                'partition_by': partition_by,
                'last_refresh': datetime.now(),
                'stats': self._collect_stats(df)
            }
            
            logger.info(f"Created materialized view: {name}")
            
        except Exception as e:
            logger.error(f"Failed to create view: {e}")
            raise
    
    def optimize_query(
        self,
        query: str,
        collect_stats: bool = True
    ) -> Dict[str, Any]:
        """
        Optimize query.
        
        Args:
            query: SQL query
            collect_stats: Whether to collect statistics
        
        Returns:
            Optimization results
        """
        try:
            # Parse query
            df = self.spark.sql(query)
            plan = df.explain(True)
            
            # Analyze plan
            analysis = self._analyze_plan(plan)
            
            # Collect statistics if needed
            if collect_stats:
                tables = self._extract_tables(query)
                for table in tables:
                    if table not in self.stats_cache:
                        self.stats_cache[table] = self._collect_table_stats(table)
            
            # Generate suggestions
            suggestions = self._generate_suggestions(analysis)
            
            return {
                'original_plan': plan,
                'analysis': analysis,
                'suggestions': suggestions
            }
            
        except Exception as e:
            logger.error(f"Failed to optimize query: {e}")
            raise
    
    def refresh_views(
        self,
        force: bool = False
    ) -> None:
        """
        Refresh materialized views.
        
        Args:
            force: Force refresh all views
        """
        try:
            for name, metadata in self.views.items():
                if force or (
                    datetime.now() - metadata['last_refresh']
                    > metadata['refresh_interval']
                ):
                    self.create_materialized_view(
                        name,
                        metadata['query'],
                        metadata['refresh_interval'],
                        metadata['partition_by']
                    )
            
        except Exception as e:
            logger.error(f"Failed to refresh views: {e}")
            raise
    
    def _collect_stats(
        self,
        df: Any
    ) -> Dict[str, Any]:
        """Collect DataFrame statistics."""
        return {
            'num_rows': df.count(),
            'num_cols': len(df.columns),
            'size_bytes': df.rdd.map(
                lambda x: len(str(x))
            ).sum()
        }
    
    def _collect_table_stats(
        self,
        table: str
    ) -> Dict[str, Any]:
        """Collect table statistics."""
        return self.spark.sql(f"""
            ANALYZE TABLE {table} COMPUTE STATISTICS
            FOR ALL COLUMNS
        """).collect()[0]
    
    def _analyze_plan(
        self,
        plan: str
    ) -> Dict[str, Any]:
        """Analyze query plan."""
        analysis = {
            'joins': [],
            'filters': [],
            'aggregations': [],
            'bottlenecks': []
        }
        
        # Analyze plan components
        if "BroadcastHashJoin" in plan:
            analysis['joins'].append("broadcast")
        if "SortMergeJoin" in plan:
            analysis['joins'].append("sort_merge")
        if "Filter" in plan:
            analysis['filters'].append("predicate_pushdown")
        
        return analysis
    
    def _generate_suggestions(
        self,
        analysis: Dict[str, Any]
    ) -> List[str]:
        """Generate optimization suggestions."""
        suggestions = []
        
        if "sort_merge" in analysis['joins']:
            suggestions.append(
                "Consider using broadcast joins for small tables"
            )
        
        if not analysis['filters']:
            suggestions.append(
                "Add filters to improve selectivity"
            )
        
        return suggestions
    
    def _extract_tables(
        self,
        query: str
    ) -> List[str]:
        """Extract table names from query."""
        # Simple extraction - can be improved
        words = query.split()
        tables = []
        
        for i, word in enumerate(words):
            if word.upper() == "FROM" or word.upper() == "JOIN":
                if i + 1 < len(words):
                    tables.append(words[i + 1])
        
        return tables 