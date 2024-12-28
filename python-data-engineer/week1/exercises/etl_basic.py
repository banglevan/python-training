"""
ETL Basic Pipeline
- Extract data từ CSV/JSON 
- Transform data đơn giản
- Load vào SQLite
"""

import pandas as pd
import sqlite3
import json
from pathlib import Path
from typing import Union, List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """Basic ETL Pipeline."""
    
    def __init__(self, db_path: str):
        """Initialize pipeline."""
        self.db_path = db_path
        self.conn = None
    
    def connect_db(self):
        """Connect to SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def extract_csv(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file."""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Extracted {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"CSV extraction error: {e}")
            raise
    
    def extract_json(self, file_path: str) -> List[Dict]:
        """Extract data from JSON file."""
        try:
            with open(file_path) as f:
                data = json.load(f)
            logger.info(f"Extracted data from {file_path}")
            return data
        except Exception as e:
            logger.error(f"JSON extraction error: {e}")
            raise
    
    def transform_data(
        self,
        data: Union[pd.DataFrame, List[Dict]]
    ) -> pd.DataFrame:
        """Transform data."""
        try:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data.copy()
            
            # Basic transformations
            # 1. Drop duplicates
            df = df.drop_duplicates()
            
            # 2. Handle missing values
            df = df.fillna({
                'numeric_col': 0,
                'string_col': 'unknown'
            })
            
            # 3. Data type conversions
            df['date_col'] = pd.to_datetime(df['date_col'])
            df['numeric_col'] = pd.to_numeric(
                df['numeric_col'], 
                errors='coerce'
            )
            
            # 4. Column renaming
            df = df.rename(columns={
                'old_name': 'new_name'
            })
            
            logger.info(f"Transformed data: {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Transform error: {e}")
            raise
    
    def load_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'replace'
    ):
        """Load data to SQLite."""
        try:
            if self.conn is None:
                self.connect_db()
            
            df.to_sql(
                table_name,
                self.conn,
                if_exists=if_exists,
                index=False
            )
            logger.info(
                f"Loaded {len(df)} rows to table: {table_name}"
            )
            
        except Exception as e:
            logger.error(f"Load error: {e}")
            raise
        
    def run_pipeline(
        self,
        input_path: str,
        table_name: str
    ):
        """Run complete ETL pipeline."""
        try:
            # Extract
            if input_path.endswith('.csv'):
                data = self.extract_csv(input_path)
            else:
                data = self.extract_json(input_path)
            
            # Transform
            transformed_data = self.transform_data(data)
            
            # Load
            self.load_data(transformed_data, table_name)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()

def main():
    """Main function."""
    # Setup paths
    input_csv = "data/input.csv"
    input_json = "data/input.json"
    db_path = "data/etl.db"
    
    # Initialize pipeline
    pipeline = ETLPipeline(db_path)
    
    # Run for CSV
    pipeline.run_pipeline(input_csv, "csv_data")
    
    # Run for JSON
    pipeline.run_pipeline(input_json, "json_data")

if __name__ == "__main__":
    main() 