"""
Dimensional Modeling
- Fact tables
- Dimension tables
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import Dict, List, Any
import logging
from datetime import datetime
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Dimension:
    """Dimension table definition."""
    name: str
    columns: Dict[str, str]
    surrogate_key: str
    natural_key: str
    attributes: List[str]

@dataclass
class Fact:
    """Fact table definition."""
    name: str
    measures: Dict[str, str]
    dimensions: List[str]
    grain: List[str]

class DimensionalModeler:
    """Dimensional modeling handler."""
    
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
    
    def create_dimension(self, dimension: Dimension):
        """Create dimension table."""
        try:
            # Generate CREATE TABLE statement
            columns = [
                f"{dimension.surrogate_key} SERIAL PRIMARY KEY"
            ]
            
            for name, dtype in dimension.columns.items():
                columns.append(f"{name} {dtype}")
            
            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {dimension.name} (
                    {', '.join(columns)}
                )
            """
            
            self.cur.execute(create_stmt)
            
            # Create index on natural key
            self.cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS 
                idx_{dimension.name}_{dimension.natural_key}
                ON {dimension.name} ({dimension.natural_key})
            """)
            
            self.conn.commit()
            logger.info(f"Created dimension: {dimension.name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Dimension creation error: {e}")
            raise
    
    def create_fact(self, fact: Fact):
        """Create fact table."""
        try:
            # Generate CREATE TABLE statement
            columns = []
            
            # Add foreign keys for dimensions
            for dim in fact.dimensions:
                columns.append(f"{dim}_key INTEGER")
            
            # Add measures
            for name, dtype in fact.measures.items():
                columns.append(f"{name} {dtype}")
            
            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {fact.name} (
                    {fact.name}_key SERIAL PRIMARY KEY,
                    {', '.join(columns)}
                )
            """
            
            self.cur.execute(create_stmt)
            
            # Create foreign key constraints
            for dim in fact.dimensions:
                self.cur.execute(f"""
                    ALTER TABLE {fact.name}
                    ADD CONSTRAINT fk_{fact.name}_{dim}
                    FOREIGN KEY ({dim}_key)
                    REFERENCES {dim} ({dim}_key)
                """)
            
            # Create indexes on foreign keys
            for dim in fact.dimensions:
                self.cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS 
                    idx_{fact.name}_{dim}
                    ON {fact.name} ({dim}_key)
                """)
            
            self.conn.commit()
            logger.info(f"Created fact: {fact.name}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Fact creation error: {e}")
            raise
    
    def load_dimension(
        self,
        dimension: Dimension,
        data: List[Dict]
    ):
        """Load data into dimension table."""
        try:
            # Prepare INSERT statement
            columns = list(dimension.columns.keys())
            placeholders = ["%s"] * len(columns)
            
            insert_stmt = f"""
                INSERT INTO {dimension.name}
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({dimension.natural_key})
                DO UPDATE SET
                {
                    ', '.join([
                        f"{col} = EXCLUDED.{col}"
                        for col in columns
                        if col != dimension.natural_key
                    ])
                }
            """
            
            # Execute batch insert
            values = [
                [row[col] for col in columns]
                for row in data
            ]
            
            self.cur.executemany(insert_stmt, values)
            self.conn.commit()
            
            logger.info(
                f"Loaded {len(data)} rows into {dimension.name}"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Dimension load error: {e}")
            raise
    
    def load_fact(
        self,
        fact: Fact,
        data: List[Dict]
    ):
        """Load data into fact table."""
        try:
            # Prepare columns
            columns = fact.dimensions + list(fact.measures.keys())
            placeholders = ["%s"] * len(columns)
            
            # Insert statement
            insert_stmt = f"""
                INSERT INTO {fact.name}
                ({', '.join([f"{col}_key" if col in fact.dimensions else col for col in columns])})
                VALUES ({', '.join(placeholders)})
            """
            
            # Execute batch insert
            values = [
                [row[col] for col in columns]
                for row in data
            ]
            
            self.cur.executemany(insert_stmt, values)
            self.conn.commit()
            
            logger.info(
                f"Loaded {len(data)} rows into {fact.name}"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Fact load error: {e}")
            raise
    
    def validate_schema(
        self,
        dimensions: List[Dimension],
        facts: List[Fact]
    ):
        """Validate dimensional schema."""
        try:
            issues = []
            
            # Check dimensions
            for dim in dimensions:
                # Verify table exists
                self.cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{dim.name}'
                    )
                """)
                if not self.cur.fetchone()['exists']:
                    issues.append(
                        f"Missing dimension table: {dim.name}"
                    )
                
                # Verify columns
                self.cur.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{dim.name}'
                """)
                columns = {
                    r['column_name']: r['data_type']
                    for r in self.cur.fetchall()
                }
                
                for col, dtype in dim.columns.items():
                    if col not in columns:
                        issues.append(
                            f"Missing column {col} in {dim.name}"
                        )
            
            # Check facts
            for fact in facts:
                # Verify table exists
                self.cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = '{fact.name}'
                    )
                """)
                if not self.cur.fetchone()['exists']:
                    issues.append(
                        f"Missing fact table: {fact.name}"
                    )
                
                # Verify foreign keys
                for dim in fact.dimensions:
                    self.cur.execute(f"""
                        SELECT EXISTS (
                            SELECT FROM information_schema.table_constraints
                            WHERE table_name = '{fact.name}'
                            AND constraint_type = 'FOREIGN KEY'
                            AND constraint_name = 'fk_{fact.name}_{dim}'
                        )
                    """)
                    if not self.cur.fetchone()['exists']:
                        issues.append(
                            f"Missing foreign key: {dim} in {fact.name}"
                        )
            
            return issues
            
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
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
    modeler = DimensionalModeler(
        dbname='dwh',
        user='dwh_user',
        password='dwh_pass'
    )
    
    try:
        # Connect to database
        modeler.connect()
        
        # Example dimension
        product_dim = Dimension(
            name='dim_product',
            columns={
                'product_id': 'VARCHAR(50)',
                'name': 'VARCHAR(100)',
                'category': 'VARCHAR(50)',
                'price': 'DECIMAL(10,2)'
            },
            surrogate_key='product_key',
            natural_key='product_id',
            attributes=['name', 'category', 'price']
        )
        
        # Example fact
        sales_fact = Fact(
            name='fact_sales',
            measures={
                'quantity': 'INTEGER',
                'amount': 'DECIMAL(12,2)'
            },
            dimensions=['product', 'customer', 'date'],
            grain=['transaction_id']
        )
        
        # Create tables
        modeler.create_dimension(product_dim)
        modeler.create_fact(sales_fact)
        
        # Validate schema
        issues = modeler.validate_schema(
            [product_dim],
            [sales_fact]
        )
        
        if issues:
            logger.warning("Schema issues found:")
            for issue in issues:
                logger.warning(f"- {issue}")
        else:
            logger.info("Schema validation passed")
        
    finally:
        modeler.disconnect()

if __name__ == '__main__':
    main() 