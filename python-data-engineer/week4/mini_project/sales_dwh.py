"""
Sales Data Warehouse
- Star schema design
- ETL pipeline
- OLAP reports
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import Dict, List, Any
import logging
from datetime import datetime
from dataclasses import dataclass
import yaml
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TableConfig:
    """Table configuration."""
    name: str
    columns: Dict[str, str]
    constraints: List[str] = None

class SalesDWH:
    """Sales Data Warehouse handler."""
    
    def __init__(
        self,
        config_path: str = 'config/dwh_config.yml'
    ):
        """Initialize from config."""
        with open(config_path) as f:
            config = yaml.safe_load(f)
            
        self.conn_params = config['database']
        self.schema = config['schema']
        self.etl_config = config['etl']
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
    
    def create_schema(self):
        """Create star schema."""
        try:
            # Create dimensions
            for dim in self.schema['dimensions']:
                table = TableConfig(**dim)
                columns = [
                    f"{name} {dtype}"
                    for name, dtype in table.columns.items()
                ]
                
                if table.constraints:
                    columns.extend(table.constraints)
                
                create_stmt = f"""
                    CREATE TABLE IF NOT EXISTS {table.name} (
                        {', '.join(columns)}
                    )
                """
                
                self.cur.execute(create_stmt)
                logger.info(f"Created dimension: {table.name}")
            
            # Create facts
            for fact in self.schema['facts']:
                table = TableConfig(**fact)
                columns = [
                    f"{name} {dtype}"
                    for name, dtype in table.columns.items()
                ]
                
                if table.constraints:
                    columns.extend(table.constraints)
                
                create_stmt = f"""
                    CREATE TABLE IF NOT EXISTS {table.name} (
                        {', '.join(columns)}
                    )
                """
                
                self.cur.execute(create_stmt)
                logger.info(f"Created fact: {table.name}")
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Schema creation error: {e}")
            raise
    
    def extract_data(self, source_config: Dict):
        """Extract data from source."""
        try:
            source_type = source_config['type']
            
            if source_type == 'csv':
                df = pd.read_csv(
                    source_config['path'],
                    **source_config.get('options', {})
                )
            elif source_type == 'database':
                source_conn = psycopg2.connect(
                    **source_config['connection']
                )
                df = pd.read_sql(
                    source_config['query'],
                    source_conn
                )
                source_conn.close()
            else:
                raise ValueError(f"Unknown source type: {source_type}")
            
            return df
            
        except Exception as e:
            logger.error(f"Data extraction error: {e}")
            raise
    
    def transform_dimension(
        self,
        df: pd.DataFrame,
        config: Dict
    ):
        """Transform dimension data."""
        try:
            # Apply mappings
            if 'mappings' in config:
                for col, mapping in config['mappings'].items():
                    df[col] = df[col].map(mapping)
            
            # Apply transformations
            if 'transformations' in config:
                for transform in config['transformations']:
                    col = transform['column']
                    func = transform['function']
                    
                    if func == 'date':
                        df[col] = pd.to_datetime(df[col])
                    elif func == 'upper':
                        df[col] = df[col].str.upper()
                    # Add more transformations as needed
            
            return df[config['columns']]
            
        except Exception as e:
            logger.error(f"Dimension transformation error: {e}")
            raise
    
    def transform_fact(
        self,
        df: pd.DataFrame,
        config: Dict
    ):
        """Transform fact data."""
        try:
            # Join with dimensions to get keys
            for join in config['joins']:
                dim_table = join['table']
                columns = join['columns']
                
                self.cur.execute(f"""
                    SELECT *
                    FROM {dim_table}
                """)
                dim_df = pd.DataFrame(
                    self.cur.fetchall()
                )
                
                df = df.merge(
                    dim_df,
                    how='left',
                    on=columns
                )
            
            # Calculate measures
            for measure in config['measures']:
                name = measure['name']
                expr = measure['expression']
                
                if expr.startswith('sum'):
                    col = expr[4:-1]  # Remove sum()
                    df[name] = df[col].sum()
                elif expr.startswith('count'):
                    col = expr[6:-1]  # Remove count()
                    df[name] = df[col].count()
                # Add more aggregations as needed
            
            return df[config['columns']]
            
        except Exception as e:
            logger.error(f"Fact transformation error: {e}")
            raise
    
    def load_data(
        self,
        table: str,
        df: pd.DataFrame,
        batch_size: int = 1000
    ):
        """Load data into table."""
        try:
            columns = list(df.columns)
            placeholders = ["%s"] * len(columns)
            
            insert_stmt = f"""
                INSERT INTO {table}
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
            """
            
            # Insert in batches
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                values = [
                    tuple(row)
                    for row in batch.values
                ]
                
                self.cur.executemany(insert_stmt, values)
                self.conn.commit()
                
                logger.info(
                    f"Loaded {len(batch)} records into {table}"
                )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Data loading error: {e}")
            raise
    
    def create_olap_reports(self):
        """Create OLAP reports."""
        try:
            reports = []
            
            # Sales by Category and Time
            self.cur.execute("""
                SELECT 
                    p.category,
                    t.year,
                    t.quarter,
                    SUM(f.amount) as total_sales,
                    COUNT(DISTINCT f.customer_key) as num_customers,
                    AVG(f.quantity) as avg_quantity
                FROM fact_sales f
                JOIN dim_product p ON f.product_key = p.product_key
                JOIN dim_time t ON f.time_key = t.time_key
                GROUP BY 
                    GROUPING SETS (
                        (p.category, t.year, t.quarter),
                        (p.category, t.year),
                        (p.category),
                        ()
                    )
                ORDER BY 
                    p.category NULLS FIRST,
                    t.year NULLS FIRST,
                    t.quarter NULLS FIRST
            """)
            
            reports.append({
                'name': 'sales_by_category_time',
                'data': self.cur.fetchall()
            })
            
            # Customer Segments Analysis
            self.cur.execute("""
                WITH customer_stats AS (
                    SELECT 
                        c.customer_key,
                        COUNT(*) as num_transactions,
                        SUM(f.amount) as total_spent,
                        AVG(f.amount) as avg_transaction
                    FROM fact_sales f
                    JOIN dim_customer c ON f.customer_key = c.customer_key
                    GROUP BY c.customer_key
                ),
                segments AS (
                    SELECT 
                        customer_key,
                        CASE 
                            WHEN total_spent > 1000 THEN 'High Value'
                            WHEN total_spent > 500 THEN 'Medium Value'
                            ELSE 'Low Value'
                        END as segment
                    FROM customer_stats
                )
                SELECT 
                    s.segment,
                    COUNT(*) as num_customers,
                    AVG(cs.total_spent) as avg_total_spent,
                    AVG(cs.num_transactions) as avg_transactions
                FROM segments s
                JOIN customer_stats cs 
                ON s.customer_key = cs.customer_key
                GROUP BY s.segment
                ORDER BY avg_total_spent DESC
            """)
            
            reports.append({
                'name': 'customer_segments',
                'data': self.cur.fetchall()
            })
            
            # Save reports
            os.makedirs('reports', exist_ok=True)
            
            for report in reports:
                df = pd.DataFrame(report['data'])
                df.to_csv(
                    f"reports/{report['name']}.csv",
                    index=False
                )
                logger.info(
                    f"Saved report: {report['name']}"
                )
            
            return reports
            
        except Exception as e:
            logger.error(f"Report creation error: {e}")
            raise
    
    def run_etl_pipeline(self):
        """Run complete ETL pipeline."""
        try:
            # Create schema
            self.create_schema()
            
            # Process dimensions
            for dim_config in self.etl_config['dimensions']:
                # Extract
                df = self.extract_data(
                    dim_config['source']
                )
                
                # Transform
                df = self.transform_dimension(
                    df,
                    dim_config
                )
                
                # Load
                self.load_data(
                    dim_config['target_table'],
                    df
                )
            
            # Process facts
            for fact_config in self.etl_config['facts']:
                # Extract
                df = self.extract_data(
                    fact_config['source']
                )
                
                # Transform
                df = self.transform_fact(
                    df,
                    fact_config
                )
                
                # Load
                self.load_data(
                    fact_config['target_table'],
                    df
                )
            
            # Create reports
            self.create_olap_reports()
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline error: {e}")
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
    dwh = SalesDWH()
    
    try:
        # Connect to database
        dwh.connect()
        
        # Run ETL pipeline
        dwh.run_etl_pipeline()
        
    finally:
        dwh.disconnect()

if __name__ == '__main__':
    main() 