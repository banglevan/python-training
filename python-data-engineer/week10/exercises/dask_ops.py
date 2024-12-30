"""
DASK operations exercise.
"""

import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, LocalCluster
import numpy as np
import pandas as pd
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DaskOperations:
    """DASK distributed computing operations."""
    
    def __init__(self, n_workers: int = 4):
        """Initialize DASK cluster."""
        self.cluster = LocalCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        logger.info(f"Initialized DASK cluster with {n_workers} workers")
    
    def load_data(
        self,
        path: str,
        file_pattern: str = "*.csv"
    ) -> dd.DataFrame:
        """Load data into DASK DataFrame."""
        try:
            df = dd.read_csv(f"{path}/{file_pattern}")
            logger.info(f"Loaded data from {path}")
            return df
        except Exception as e:
            logger.error(f"Data loading failed: {e}")
            raise
    
    def process_dataframe(
        self,
        df: dd.DataFrame,
        operations: List[Dict[str, Any]]
    ) -> dd.DataFrame:
        """Process DASK DataFrame with operations."""
        try:
            for op in operations:
                op_type = op['type']
                
                if op_type == 'filter':
                    df = df[df[op['column']].isin(op['values'])]
                
                elif op_type == 'transform':
                    df[op['target']] = df[op['source']].map_partitions(
                        lambda x: eval(op['expression'])
                    )
                
                elif op_type == 'groupby':
                    df = df.groupby(op['columns']).agg(op['aggregations'])
                
                elif op_type == 'sort':
                    df = df.sort_values(op['columns'])
            
            logger.info("DataFrame processing completed")
            return df
            
        except Exception as e:
            logger.error(f"DataFrame processing failed: {e}")
            raise
    
    def matrix_operations(
        self,
        shape: tuple,
        chunk_size: tuple,
        operation: str
    ) -> da.Array:
        """Perform distributed matrix operations."""
        try:
            # Create random matrix
            matrix = da.random.random(
                shape,
                chunks=chunk_size
            )
            
            if operation == 'transpose':
                result = matrix.T
            
            elif operation == 'inverse':
                result = da.linalg.inv(matrix)
            
            elif operation == 'svd':
                u, s, v = da.linalg.svd(matrix)
                result = u
            
            else:
                raise ValueError(f"Unsupported operation: {operation}")
            
            logger.info(f"Matrix operation '{operation}' completed")
            return result
            
        except Exception as e:
            logger.error(f"Matrix operation failed: {e}")
            raise
    
    def time_series_analysis(
        self,
        df: dd.DataFrame,
        date_column: str,
        value_column: str,
        freq: str = 'D'
    ) -> pd.DataFrame:
        """Perform time series analysis."""
        try:
            # Set index
            df = df.set_index(date_column)
            
            # Resample and compute statistics
            stats = df[value_column].resample(freq).agg([
                'mean',
                'std',
                'min',
                'max',
                'count'
            ])
            
            # Compute rolling metrics
            rolling = df[value_column].rolling(window='7D').mean()
            
            result = stats.compute()  # Convert to pandas
            result['rolling_avg'] = rolling.compute()
            
            logger.info("Time series analysis completed")
            return result
            
        except Exception as e:
            logger.error(f"Time series analysis failed: {e}")
            raise
    
    def parallel_apply(
        self,
        df: dd.DataFrame,
        func: callable,
        column: str
    ) -> dd.Series:
        """Apply function in parallel."""
        try:
            result = df[column].map_partitions(func)
            logger.info(f"Parallel apply completed on column {column}")
            return result
            
        except Exception as e:
            logger.error(f"Parallel apply failed: {e}")
            raise
    
    def cleanup(self):
        """Cleanup DASK cluster."""
        try:
            self.client.close()
            self.cluster.close()
            logger.info("DASK cluster cleaned up")
        except Exception as e:
            logger.error(f"Cluster cleanup failed: {e}")
            raise

if __name__ == "__main__":
    # Example usage
    dask_ops = DaskOperations(n_workers=4)
    
    try:
        # Load data
        df = dask_ops.load_data("data/")
        
        # Process DataFrame
        operations = [
            {
                'type': 'filter',
                'column': 'category',
                'values': ['A', 'B']
            },
            {
                'type': 'transform',
                'source': 'value',
                'target': 'value_squared',
                'expression': 'x ** 2'
            },
            {
                'type': 'groupby',
                'columns': ['category'],
                'aggregations': {'value': 'mean'}
            }
        ]
        
        result_df = dask_ops.process_dataframe(df, operations)
        print(result_df.compute())
        
        # Matrix operations
        matrix = dask_ops.matrix_operations(
            shape=(1000, 1000),
            chunk_size=(100, 100),
            operation='svd'
        )
        print(matrix.compute())
        
    finally:
        dask_ops.cleanup() 