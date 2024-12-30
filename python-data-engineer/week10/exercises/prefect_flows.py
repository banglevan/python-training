"""
Prefect flows exercise.
"""

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.logging import get_run_logger
from datetime import timedelta
import pandas as pd
import numpy as np
import requests
from typing import List, Dict, Any
import json

@task(
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def extract_data(url: str) -> pd.DataFrame:
    """Extract data from API."""
    logger = get_run_logger()
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        logger.info(f"Extracted {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

@task(retries=2)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data."""
    logger = get_run_logger()
    try:
        # Clean data
        df = df.dropna()
        
        # Convert dates
        date_columns = df.select_dtypes(include=['object']).columns
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass
        
        # Calculate metrics
        if 'amount' in df.columns:
            df['amount_usd'] = df['amount'] * 1.2  # Example conversion
        
        logger.info(f"Transformed {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

@task
def validate_data(df: pd.DataFrame) -> bool:
    """Validate data quality."""
    logger = get_run_logger()
    try:
        # Check for nulls
        null_counts = df.isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values: {null_counts[null_counts > 0]}")
        
        # Check data types
        logger.info(f"Data types: {df.dtypes}")
        
        # Check value ranges
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            min_val = df[col].min()
            max_val = df[col].max()
            logger.info(f"{col} range: {min_val} to {max_val}")
        
        return True
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return False

@task
def load_data(df: pd.DataFrame, output_path: str) -> None:
    """Load data to destination."""
    logger = get_run_logger()
    try:
        # Save to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise

def on_completion(flow, state):
    """Flow completion handler."""
    logger = get_run_logger()
    logger.info(f"Flow {flow.name} finished with state: {state.name}")

def on_failure(flow, state):
    """Flow failure handler."""
    logger = get_run_logger()
    logger.error(f"Flow {flow.name} failed with state: {state.name}")
    # Could add notification logic here

@flow(
    name="data_processing",
    description="Extract, transform and load data",
    version="1.0",
    on_completion=[on_completion],
    on_failure=[on_failure]
)
def process_data(
    source_url: str,
    output_path: str
) -> None:
    """Main data processing flow."""
    # Extract
    df = extract_data(source_url)
    
    # Transform
    df_transformed = transform_data(df)
    
    # Validate
    is_valid = validate_data(df_transformed)
    if not is_valid:
        raise ValueError("Data validation failed")
    
    # Load
    load_data(df_transformed, output_path)

if __name__ == "__main__":
    # Run flow
    process_data(
        source_url="https://api.example.com/data",
        output_path="output/data.csv"
    ) 