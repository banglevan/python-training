"""
DWH Operations
- Slowly changing dimensions
- Incremental loads
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import Dict, List, Any
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SCDType(Enum):
    """SCD implementation types."""
    TYPE1 = 1  # Overwrite
    TYPE2 = 2  # Add new version
    TYPE3 = 3  # Add new attribute
    TYPE4 = 4  # Add history table

@dataclass
class SCDConfig:
    """SCD configuration."""
    type: SCDType
    track_columns: List[str]
    effective_date: str = 'effective_date'
    end_date: str = 'end_date'
    current_flag: str = 'is_current'
    version: str = 'version'

class DWHOperations:
    """Data Warehouse operations handler."""
    
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
    
    def handle_scd(
        self,
        table: str,
        data: List[Dict],
        natural_key: str,
        config: SCDConfig
    ):
        """Handle slowly changing dimension."""
        try:
            if config.type == SCDType.TYPE1:
                self._handle_scd1(table, data, natural_key)
            
            elif config.type == SCDType.TYPE2:
                self._handle_scd2(
                    table,
                    data,
                    natural_key,
                    config
                )
            
            elif config.type == SCDType.TYPE3:
                self._handle_scd3(
                    table,
                    data,
                    natural_key,
                    config
                )
            
            elif config.type == SCDType.TYPE4:
                self._handle_scd4(
                    table,
                    data,
                    natural_key,
                    config
                )
            
            self.conn.commit()
            logger.info(
                f"Processed {len(data)} records for {table}"
            )
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"SCD processing error: {e}")
            raise
    
    def _handle_scd1(
        self,
        table: str,
        data: List[Dict],
        natural_key: str
    ):
        """Handle Type 1 SCD - Overwrite."""
        columns = list(data[0].keys())
        placeholders = ["%s"] * len(columns)
        
        # Upsert statement
        upsert_stmt = f"""
            INSERT INTO {table}
            ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({natural_key})
            DO UPDATE SET
            {
                ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col != natural_key
                ])
            }
        """
        
        values = [
            [row[col] for col in columns]
            for row in data
        ]
        
        self.cur.executemany(upsert_stmt, values)
    
    def _handle_scd2(
        self,
        table: str,
        data: List[Dict],
        natural_key: str,
        config: SCDConfig
    ):
        """Handle Type 2 SCD - Add new version."""
        now = datetime.now()
        
        for record in data:
            # Check if record exists
            self.cur.execute(f"""
                SELECT *
                FROM {table}
                WHERE {natural_key} = %s
                AND {config.current_flag} = true
            """, (record[natural_key],))
            
            current = self.cur.fetchone()
            
            if current:
                # Check if tracked columns changed
                changed = any(
                    record[col] != current[col]
                    for col in config.track_columns
                )
                
                if changed:
                    # Expire current record
                    self.cur.execute(f"""
                        UPDATE {table}
                        SET 
                            {config.end_date} = %s,
                            {config.current_flag} = false
                        WHERE {natural_key} = %s
                        AND {config.current_flag} = true
                    """, (now, record[natural_key]))
                    
                    # Insert new version
                    columns = list(record.keys()) + [
                        config.effective_date,
                        config.end_date,
                        config.current_flag,
                        config.version
                    ]
                    
                    values = list(record.values()) + [
                        now,
                        None,
                        True,
                        current[config.version] + 1
                    ]
                    
                    placeholders = ["%s"] * len(columns)
                    
                    self.cur.execute(f"""
                        INSERT INTO {table}
                        ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                    """, values)
            
            else:
                # Insert new record
                columns = list(record.keys()) + [
                    config.effective_date,
                    config.end_date,
                    config.current_flag,
                    config.version
                ]
                
                values = list(record.values()) + [
                    now,
                    None,
                    True,
                    1
                ]
                
                placeholders = ["%s"] * len(columns)
                
                self.cur.execute(f"""
                    INSERT INTO {table}
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """, values)
    
    def _handle_scd3(
        self,
        table: str,
        data: List[Dict],
        natural_key: str,
        config: SCDConfig
    ):
        """Handle Type 3 SCD - Add new attribute."""
        for record in data:
            # Check if record exists
            self.cur.execute(f"""
                SELECT *
                FROM {table}
                WHERE {natural_key} = %s
            """, (record[natural_key],))
            
            current = self.cur.fetchone()
            
            if current:
                # Update with previous values
                updates = []
                values = []
                
                for col in config.track_columns:
                    if record[col] != current[col]:
                        updates.append(
                            f"previous_{col} = %s, "
                            f"current_{col} = %s"
                        )
                        values.extend([
                            current[f"current_{col}"],
                            record[col]
                        ])
                
                if updates:
                    values.append(record[natural_key])
                    
                    self.cur.execute(f"""
                        UPDATE {table}
                        SET {', '.join(updates)}
                        WHERE {natural_key} = %s
                    """, values)
            
            else:
                # Insert new record
                columns = [natural_key]
                values = [record[natural_key]]
                
                for col in config.track_columns:
                    columns.extend([
                        f"previous_{col}",
                        f"current_{col}"
                    ])
                    values.extend([None, record[col]])
                
                placeholders = ["%s"] * len(columns)
                
                self.cur.execute(f"""
                    INSERT INTO {table}
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """, values)
    
    def _handle_scd4(
        self,
        table: str,
        data: List[Dict],
        natural_key: str,
        config: SCDConfig
    ):
        """Handle Type 4 SCD - Add history table."""
        history_table = f"{table}_history"
        
        # Ensure history table exists
        self.cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {history_table} (
                history_id SERIAL PRIMARY KEY,
                {natural_key} VARCHAR(50),
                {
                    ', '.join([
                        f"{col} VARCHAR(255)"
                        for col in config.track_columns
                    ])
                },
                effective_date TIMESTAMP,
                end_date TIMESTAMP,
                FOREIGN KEY ({natural_key})
                REFERENCES {table}({natural_key})
            )
        """)
        
        now = datetime.now()
        
        for record in data:
            # Update current record
            columns = list(record.keys())
            placeholders = ["%s"] * len(columns)
            values = [record[col] for col in columns]
            
            self.cur.execute(f"""
                INSERT INTO {table}
                ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                ON CONFLICT ({natural_key})
                DO UPDATE SET
                {
                    ', '.join([
                        f"{col} = EXCLUDED.{col}"
                        for col in columns
                        if col != natural_key
                    ])
                }
                RETURNING *
            """, values)
            
            updated = self.cur.fetchone()
            
            # Add to history if tracked columns changed
            self.cur.execute(f"""
                SELECT *
                FROM {history_table}
                WHERE {natural_key} = %s
                AND end_date IS NULL
            """, (record[natural_key],))
            
            current_history = self.cur.fetchone()
            
            changed = False
            if current_history:
                changed = any(
                    updated[col] != current_history[col]
                    for col in config.track_columns
                )
            
            if changed or not current_history:
                if current_history:
                    # Close current history record
                    self.cur.execute(f"""
                        UPDATE {history_table}
                        SET end_date = %s
                        WHERE history_id = %s
                    """, (now, current_history['history_id']))
                
                # Insert new history record
                columns = [natural_key] + \
                         config.track_columns + \
                         ['effective_date']
                         
                values = [
                    updated[natural_key]
                ] + [
                    updated[col]
                    for col in config.track_columns
                ] + [now]
                
                placeholders = ["%s"] * len(columns)
                
                self.cur.execute(f"""
                    INSERT INTO {history_table}
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """, values)
    
    def load_incremental(
        self,
        target_table: str,
        source_data: List[Dict],
        key_column: str,
        timestamp_column: str
    ):
        """Handle incremental load."""
        try:
            # Get last loaded timestamp
            self.cur.execute(f"""
                SELECT MAX({timestamp_column})
                FROM {target_table}
            """)
            last_timestamp = self.cur.fetchone()['max']
            
            if last_timestamp:
                # Filter new records
                new_data = [
                    record for record in source_data
                    if record[timestamp_column] > last_timestamp
                ]
            else:
                new_data = source_data
            
            if new_data:
                # Insert new records
                columns = list(new_data[0].keys())
                placeholders = ["%s"] * len(columns)
                
                insert_stmt = f"""
                    INSERT INTO {target_table}
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                
                values = [
                    [row[col] for col in columns]
                    for row in new_data
                ]
                
                self.cur.executemany(insert_stmt, values)
                self.conn.commit()
                
                logger.info(
                    f"Loaded {len(new_data)} new records"
                )
            else:
                logger.info("No new records to load")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Incremental load error: {e}")
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
    dwh = DWHOperations(
        dbname='dwh',
        user='dwh_user',
        password='dwh_pass'
    )
    
    try:
        # Connect to database
        dwh.connect()
        
        # Example SCD Type 2
        product_updates = [
            {
                'product_id': 'P1',
                'name': 'Updated Product 1',
                'category': 'Electronics',
                'price': 199.99
            }
        ]
        
        scd2_config = SCDConfig(
            type=SCDType.TYPE2,
            track_columns=['name', 'category', 'price']
        )
        
        dwh.handle_scd(
            'dim_product',
            product_updates,
            'product_id',
            scd2_config
        )
        
        # Example incremental load
        new_sales = [
            {
                'sale_id': 'S1001',
                'product_id': 'P1',
                'quantity': 2,
                'amount': 399.98,
                'created_at': datetime.now()
            }
        ]
        
        dwh.load_incremental(
            'fact_sales',
            new_sales,
            'sale_id',
            'created_at'
        )
        
    finally:
        dwh.disconnect()

if __name__ == '__main__':
    main() 