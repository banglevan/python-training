"""
Query optimization for Trino.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import sqlparse
from ..federation.query_planner import QueryPlanner

logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Optimize Trino queries."""
    
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize query optimizer.
        
        Args:
            config: Optimizer configuration
        """
        self.config = config or {}
        self.planner = QueryPlanner()
    
    def optimize_query(
        self,
        query: str,
        stats: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Optimize SQL query.
        
        Args:
            query: SQL query
            stats: Optional table statistics
            
        Returns:
            Optimized query
        """
        try:
            logger.info("Optimizing query")
            
            # Parse query
            parsed = sqlparse.parse(query)[0]
            
            # Apply optimizations
            optimized = self._apply_optimizations(parsed, stats)
            
            # Format query
            final_query = str(optimized)
            
            logger.info("Query optimization completed")
            return final_query
            
        except Exception as e:
            logger.error(f"Query optimization failed: {e}")
            raise
    
    def _apply_optimizations(
        self,
        parsed_query: sqlparse.sql.Statement,
        stats: Optional[Dict[str, Any]] = None
    ) -> sqlparse.sql.Statement:
        """Apply optimization rules."""
        try:
            # Predicate pushdown
            if self.config.get('enable_predicate_pushdown', True):
                parsed_query = self._push_down_predicates(
                    parsed_query
                )
            
            # Column pruning
            if self.config.get('enable_column_pruning', True):
                parsed_query = self._prune_columns(
                    parsed_query
                )
            
            # Join reordering
            if (
                self.config.get('enable_join_reordering', True)
                and stats
            ):
                parsed_query = self._reorder_joins(
                    parsed_query, stats
                )
            
            # Partition pruning
            if self.config.get('enable_partition_pruning', True):
                parsed_query = self._prune_partitions(
                    parsed_query
                )
            
            return parsed_query
            
        except Exception as e:
            logger.error(f"Failed to apply optimizations: {e}")
            raise
    
    def _push_down_predicates(
        self,
        query: sqlparse.sql.Statement
    ) -> sqlparse.sql.Statement:
        """Push down WHERE clauses."""
        try:
            # Find WHERE clauses
            where_clauses = []
            for token in query.tokens:
                if (
                    isinstance(token, sqlparse.sql.Where)
                    and token.tokens
                ):
                    where_clauses.extend(
                        self._split_conditions(token)
                    )
            
            # Push down to subqueries
            if where_clauses:
                query = self._inject_predicates(
                    query, where_clauses
                )
            
            return query
            
        except Exception as e:
            logger.error(
                f"Failed to push down predicates: {e}"
            )
            raise
    
    def _prune_columns(
        self,
        query: sqlparse.sql.Statement
    ) -> sqlparse.sql.Statement:
        """Remove unused columns."""
        try:
            # Find referenced columns
            referenced = self._find_referenced_columns(query)
            
            # Update SELECT clauses
            query = self._update_projections(
                query, referenced
            )
            
            return query
            
        except Exception as e:
            logger.error(f"Failed to prune columns: {e}")
            raise
    
    def _reorder_joins(
        self,
        query: sqlparse.sql.Statement,
        stats: Dict[str, Any]
    ) -> sqlparse.sql.Statement:
        """Reorder JOIN clauses."""
        try:
            # Extract join graph
            joins = self._extract_joins(query)
            
            if not joins:
                return query
            
            # Calculate optimal order
            optimal_order = self.planner.plan_joins(
                joins, stats
            )
            
            # Rewrite query
            return self._rewrite_with_joins(
                query, optimal_order
            )
            
        except Exception as e:
            logger.error(f"Failed to reorder joins: {e}")
            raise
    
    def _prune_partitions(
        self,
        query: sqlparse.sql.Statement
    ) -> sqlparse.sql.Statement:
        """Add partition filters."""
        try:
            # Find partition columns
            partitions = self._find_partition_columns(query)
            
            if not partitions:
                return query
            
            # Add partition filters
            return self._add_partition_filters(
                query, partitions
            )
            
        except Exception as e:
            logger.error(f"Failed to prune partitions: {e}")
            raise 