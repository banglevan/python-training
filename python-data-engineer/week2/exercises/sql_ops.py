"""
SQL Operations
- CRUD operations
- Joins & aggregations 
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Any
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseOperations:
    """Database operations handler."""
    
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = 'localhost',
        port: int = 5432
    ):
        """Initialize database connection."""
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
    
    def create_record(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> int:
        """Create a new record."""
        try:
            columns = ', '.join(data.keys())
            values = ', '.join(['%s'] * len(data))
            query = f"""
                INSERT INTO {table} ({columns})
                VALUES ({values})
                RETURNING id
            """
            
            self.cur.execute(query, list(data.values()))
            record_id = self.cur.fetchone()['id']
            self.conn.commit()
            
            logger.info(
                f"Created record in {table} with id {record_id}"
            )
            return record_id
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Create error: {e}")
            raise
    
    def read_records(
        self,
        table: str,
        columns: List[str] = None,
        conditions: Dict[str, Any] = None,
        order_by: str = None,
        limit: int = None
    ) -> List[Dict]:
        """Read records with conditions."""
        try:
            # Build SELECT clause
            select_cols = '*'
            if columns:
                select_cols = ', '.join(columns)
            
            query = f"SELECT {select_cols} FROM {table}"
            
            # Build WHERE clause
            params = []
            if conditions:
                where_clauses = []
                for key, value in conditions.items():
                    where_clauses.append(f"{key} = %s")
                    params.append(value)
                query += " WHERE " + " AND ".join(where_clauses)
            
            # Add ORDER BY
            if order_by:
                query += f" ORDER BY {order_by}"
            
            # Add LIMIT
            if limit:
                query += f" LIMIT {limit}"
            
            self.cur.execute(query, params)
            records = self.cur.fetchall()
            
            logger.info(
                f"Retrieved {len(records)} records from {table}"
            )
            return records
            
        except Exception as e:
            logger.error(f"Read error: {e}")
            raise
    
    def update_record(
        self,
        table: str,
        record_id: int,
        data: Dict[str, Any]
    ) -> bool:
        """Update an existing record."""
        try:
            set_clauses = [
                f"{key} = %s" 
                for key in data.keys()
            ]
            query = f"""
                UPDATE {table}
                SET {', '.join(set_clauses)}
                WHERE id = %s
            """
            
            params = list(data.values()) + [record_id]
            self.cur.execute(query, params)
            rows_affected = self.cur.rowcount
            self.conn.commit()
            
            logger.info(
                f"Updated {rows_affected} records in {table}"
            )
            return rows_affected > 0
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Update error: {e}")
            raise
    
    def delete_record(
        self,
        table: str,
        record_id: int
    ) -> bool:
        """Delete a record."""
        try:
            query = f"""
                DELETE FROM {table}
                WHERE id = %s
            """
            
            self.cur.execute(query, [record_id])
            rows_affected = self.cur.rowcount
            self.conn.commit()
            
            logger.info(
                f"Deleted {rows_affected} records from {table}"
            )
            return rows_affected > 0
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Delete error: {e}")
            raise
    
    def execute_join(
        self,
        tables: List[str],
        join_conditions: List[str],
        columns: List[str] = None,
        where_clause: str = None,
        params: List[Any] = None
    ) -> List[Dict]:
        """Execute a JOIN query."""
        try:
            # Build SELECT clause
            select_cols = '*'
            if columns:
                select_cols = ', '.join(columns)
            
            # Build FROM and JOIN clauses
            from_clause = tables[0]
            join_clause = ' '.join([
                f"JOIN {table} ON {condition}"
                for table, condition 
                in zip(tables[1:], join_conditions)
            ])
            
            query = f"""
                SELECT {select_cols}
                FROM {from_clause}
                {join_clause}
            """
            
            # Add WHERE clause if provided
            if where_clause:
                query += f" WHERE {where_clause}"
            
            self.cur.execute(query, params or [])
            results = self.cur.fetchall()
            
            logger.info(
                f"Executed join query, retrieved {len(results)} records"
            )
            return results
            
        except Exception as e:
            logger.error(f"Join error: {e}")
            raise
    
    def execute_aggregation(
        self,
        table: str,
        group_by: List[str],
        aggregations: Dict[str, str],
        having: str = None,
        params: List[Any] = None
    ) -> List[Dict]:
        """Execute an aggregation query."""
        try:
            # Build SELECT clause with aggregations
            select_items = group_by.copy()
            for col, agg in aggregations.items():
                select_items.append(f"{agg}({col})")
            
            query = f"""
                SELECT {', '.join(select_items)}
                FROM {table}
                GROUP BY {', '.join(group_by)}
            """
            
            # Add HAVING clause if provided
            if having:
                query += f" HAVING {having}"
            
            self.cur.execute(query, params or [])
            results = self.cur.fetchall()
            
            logger.info(
                f"Executed aggregation query, "
                f"retrieved {len(results)} groups"
            )
            return results
            
        except Exception as e:
            logger.error(f"Aggregation error: {e}")
            raise

def main():
    """Main function."""
    # Initialize database operations
    db = DatabaseOperations(
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )
    
    try:
        # Connect to database
        db.connect()
        
        # Example CRUD operations
        user_data = {
            'name': 'John Doe',
            'email': 'john@example.com',
            'created_at': datetime.now()
        }
        
        # Create
        user_id = db.create_record('users', user_data)
        
        # Read
        users = db.read_records(
            'users',
            columns=['id', 'name', 'email'],
            conditions={'id': user_id}
        )
        
        # Update
        db.update_record(
            'users',
            user_id,
            {'name': 'John Smith'}
        )
        
        # Delete
        db.delete_record('users', user_id)
        
        # Example JOIN
        orders = db.execute_join(
            tables=['orders', 'users'],
            join_conditions=['orders.user_id = users.id'],
            columns=[
                'orders.id',
                'users.name',
                'orders.total'
            ],
            where_clause='orders.total > %s',
            params=[100]
        )
        
        # Example aggregation
        sales = db.execute_aggregation(
            table='orders',
            group_by=['user_id'],
            aggregations={
                'total': 'SUM',
                'id': 'COUNT'
            },
            having='SUM(total) > %s',
            params=[1000]
        )
        
    finally:
        # Close connection
        db.disconnect()

if __name__ == '__main__':
    main() 