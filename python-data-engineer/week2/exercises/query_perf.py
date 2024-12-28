"""
Query Performance
- Query plans
- Optimization techniques
- Benchmarking
"""

import psycopg2
import time
from typing import Dict, List, Any
import logging
from dataclasses import dataclass
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class QueryPlan:
    """Query execution plan details."""
    query: str
    plan: Dict
    execution_time: float
    rows_affected: int
    indexes_used: List[str]

class QueryOptimizer:
    """Query optimization handler."""
    
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
        self.query_history = []
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cur = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Disconnection error: {e}")
            raise
    
    def analyze_query(self, query: str) -> QueryPlan:
        """Analyze query execution plan."""
        try:
            # Get query plan
            self.cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            plan = self.cur.fetchone()[0]
            
            # Execute query with timing
            start_time = time.time()
            self.cur.execute(query)
            execution_time = time.time() - start_time
            
            # Get affected rows
            rows_affected = self.cur.rowcount
            
            # Extract used indexes
            indexes_used = self._extract_indexes_from_plan(plan)
            
            query_plan = QueryPlan(
                query=query,
                plan=plan,
                execution_time=execution_time,
                rows_affected=rows_affected,
                indexes_used=indexes_used
            )
            
            self.query_history.append(query_plan)
            return query_plan
            
        except Exception as e:
            logger.error(f"Query analysis error: {e}")
            raise
    
    def _extract_indexes_from_plan(
        self,
        plan: Dict
    ) -> List[str]:
        """Extract used indexes from query plan."""
        indexes = []
        
        def traverse_plan(node):
            if 'Index Name' in node:
                indexes.append(node['Index Name'])
            
            if 'Plans' in node:
                for subplan in node['Plans']:
                    traverse_plan(subplan)
        
        traverse_plan(plan[0]['Plan'])
        return indexes
    
    def benchmark_query(
        self,
        query: str,
        iterations: int = 5
    ) -> Dict:
        """Benchmark query performance."""
        try:
            execution_times = []
            
            for _ in range(iterations):
                # Clear caches
                self.cur.execute(
                    "DISCARD ALL"
                )
                
                # Execute with timing
                start_time = time.time()
                self.cur.execute(query)
                execution_time = time.time() - start_time
                
                execution_times.append(execution_time)
            
            return {
                'min_time': min(execution_times),
                'max_time': max(execution_times),
                'avg_time': sum(execution_times) / len(execution_times),
                'iterations': iterations
            }
            
        except Exception as e:
            logger.error(f"Benchmark error: {e}")
            raise
    
    def suggest_optimizations(
        self,
        query: str
    ) -> List[str]:
        """Suggest query optimizations."""
        try:
            suggestions = []
            
            # Analyze query
            self.cur.execute(f"EXPLAIN {query}")
            plan = self.cur.fetchall()
            
            # Check for sequential scans
            if any('Seq Scan' in str(row) for row in plan):
                suggestions.append(
                    "Consider adding indexes to avoid sequential scans"
                )
            
            # Check for large sorts
            if any('Sort' in str(row) for row in plan):
                suggestions.append(
                    "Large sort operations detected. "
                    "Consider adding indexes or optimizing ORDER BY"
                )
            
            # Check for nested loops
            if any('Nested Loop' in str(row) for row in plan):
                suggestions.append(
                    "Nested loops detected. "
                    "Consider optimizing JOIN conditions"
                )
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Optimization suggestion error: {e}")
            raise
    
    def create_performance_report(
        self,
        output_file: str
    ):
        """Generate performance report."""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'queries': []
            }
            
            for query_plan in self.query_history:
                query_report = {
                    'query': query_plan.query,
                    'execution_time': query_plan.execution_time,
                    'rows_affected': query_plan.rows_affected,
                    'indexes_used': query_plan.indexes_used,
                    'suggestions': self.suggest_optimizations(
                        query_plan.query
                    )
                }
                
                # Add benchmark if execution time is high
                if query_plan.execution_time > 1.0:
                    query_report['benchmark'] = self.benchmark_query(
                        query_plan.query
                    )
                
                report['queries'].append(query_report)
            
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Performance report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Report generation error: {e}")
            raise

def main():
    """Main function."""
    # Initialize optimizer
    optimizer = QueryOptimizer(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )
    
    try:
        # Connect to database
        optimizer.connect()
        
        # Example queries to analyze
        queries = [
            """
            SELECT u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY order_count DESC
            """,
            
            """
            SELECT p.name, SUM(oi.quantity * oi.price) as revenue
            FROM products p
            JOIN order_items oi ON p.id = oi.product_id
            WHERE oi.created_at >= NOW() - INTERVAL '30 days'
            GROUP BY p.name
            HAVING SUM(oi.quantity * oi.price) > 1000
            """
        ]
        
        # Analyze each query
        for query in queries:
            plan = optimizer.analyze_query(query)
            logger.info(
                f"Query execution time: {plan.execution_time:.3f}s"
            )
            logger.info(f"Rows affected: {plan.rows_affected}")
            logger.info(f"Indexes used: {plan.indexes_used}")
            
            # Get optimization suggestions
            suggestions = optimizer.suggest_optimizations(query)
            for suggestion in suggestions:
                logger.info(f"Suggestion: {suggestion}")
            
            # Benchmark if needed
            if plan.execution_time > 1.0:
                benchmark = optimizer.benchmark_query(query)
                logger.info(f"Benchmark results: {benchmark}")
        
        # Generate performance report
        optimizer.create_performance_report(
            'query_performance.json'
        )
        
    finally:
        # Close connection
        optimizer.disconnect()

if __name__ == '__main__':
    main() 