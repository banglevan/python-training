"""
Storage format optimization exercises.
"""

from typing import Dict, Any, Optional, List, Union
import logging
from abc import ABC, abstractmethod
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParquetOptimizer:
    """Parquet optimization management."""
    
    def __init__(self, spark: SparkSession):
        """Initialize optimizer."""
        self.spark = spark
    
    def optimize_schema(
        self,
        df: Any,
        compression: str = 'snappy',
        row_group_size: int = 128 * 1024 * 1024  # 128MB
    ) -> Any:
        """
        Optimize Parquet schema.
        
        Args:
            df: Input DataFrame
            compression: Compression codec
            row_group_size: Row group size in bytes
        
        Returns:
            Optimized DataFrame
        """
        try:
            # Set Parquet options
            return df.write \
                .option("compression", compression) \
                .option("parquet.block.size", row_group_size) \
                .option("parquet.enable.dictionary", "true") \
                .option("parquet.page.size", "1048576") # 1MB
                
        except Exception as e:
            logger.error(f"Failed to optimize schema: {e}")
            raise

    def create_vector_index(
        self,
        table_path: str,
        column: str,
        vector_dim: int = 128
    ) -> None:
        """
        Create vector index for embeddings.
        
        Args:
            table_path: Path to table
            column: Column name
            vector_dim: Vector dimension
        """
        try:
            # Read Parquet file
            table = pq.read_table(table_path)
            
            # Extract vectors
            vectors = table[column].to_numpy()
            vectors = vectors.reshape(-1, vector_dim)
            
            # Create index using FAISS
            import faiss
            index = faiss.IndexFlatL2(vector_dim)
            index.add(vectors.astype(np.float32))
            
            # Save index
            faiss.write_index(
                index,
                f"{table_path}_vector_index"
            )
            
            logger.info(f"Created vector index for {column}")
            
        except Exception as e:
            logger.error(f"Failed to create vector index: {e}")
            raise
    
    def optimize_compression(
        self,
        df: Any,
        columns: Dict[str, str]
    ) -> Any:
        """
        Optimize compression by column.
        
        Args:
            df: Input DataFrame
            columns: Column compression mappings
        
        Returns:
            Optimized DataFrame
        """
        try:
            writer = df.write
            
            for column, codec in columns.items():
                writer = writer.option(
                    f"parquet.compression.codec.{column}",
                    codec
                )
            
            return writer
            
        except Exception as e:
            logger.error(f"Failed to optimize compression: {e}")
            raise
    
    def analyze_storage(
        self,
        table_path: str
    ) -> Dict[str, Any]:
        """
        Analyze storage characteristics.
        
        Args:
            table_path: Path to table
        
        Returns:
            Storage analysis
        """
        try:
            # Read Parquet metadata
            metadata = pq.read_metadata(table_path)
            
            analysis = {
                'num_row_groups': metadata.num_row_groups,
                'num_rows': metadata.num_rows,
                'size_bytes': metadata.serialized_size,
                'compression': {},
                'encoding': {},
                'column_sizes': {}
            }
            
            # Analyze each column
            for i in range(metadata.num_columns):
                col = metadata.schema.names[i]
                analysis['compression'][col] = []
                analysis['encoding'][col] = []
                analysis['column_sizes'][col] = 0
                
                for j in range(metadata.num_row_groups):
                    col_meta = metadata.row_group(j).column(i)
                    analysis['compression'][col].append(
                        col_meta.compression
                    )
                    analysis['encoding'][col].append(
                        [enc.name for enc in col_meta.encodings]
                    )
                    analysis['column_sizes'][col] += col_meta.total_compressed_size
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze storage: {e}")
            raise

class CompressionStrategy:
    """Compression strategy management."""
    
    @staticmethod
    def suggest_compression(
        column_stats: Dict[str, Any]
    ) -> str:
        """
        Suggest compression codec.
        
        Args:
            column_stats: Column statistics
        
        Returns:
            Suggested compression codec
        """
        try:
            # Get column characteristics
            dtype = column_stats['dtype']
            unique_ratio = column_stats['unique_count'] / column_stats['total_count']
            avg_length = column_stats.get('avg_length', 0)
            
            # Suggest compression
            if dtype in ['int32', 'int64', 'float32', 'float64']:
                return 'ZSTD'  # Good for numeric data
            
            elif dtype == 'string':
                if unique_ratio > 0.8:  # High cardinality
                    return 'ZSTD'
                else:  # Low cardinality
                    return 'DICTIONARY,ZSTD'
            
            elif dtype == 'binary':
                if avg_length > 1024:  # Large binary objects
                    return 'GZIP'
                else:
                    return 'SNAPPY'
            
            else:
                return 'SNAPPY'  # Default
                
        except Exception as e:
            logger.error(f"Failed to suggest compression: {e}")
            raise
    
    @staticmethod
    def estimate_savings(
        current_size: int,
        codec: str,
        sample_data: Any
    ) -> Dict[str, Any]:
        """
        Estimate compression savings.
        
        Args:
            current_size: Current size in bytes
            codec: Compression codec
            sample_data: Sample data
        
        Returns:
            Savings estimate
        """
        try:
            # Create small Parquet file with sample
            import tempfile
            with tempfile.NamedTemporaryFile() as tmp:
                # Write with new codec
                pq.write_table(
                    sample_data,
                    tmp.name,
                    compression=codec
                )
                
                # Get compressed size
                compressed_size = tmp.tell()
            
            # Calculate savings
            savings = {
                'original_size': current_size,
                'compressed_size': compressed_size,
                'ratio': compressed_size / current_size,
                'savings_bytes': current_size - compressed_size,
                'savings_percent': (
                    (current_size - compressed_size) / current_size
                ) * 100
            }
            
            return savings
            
        except Exception as e:
            logger.error(f"Failed to estimate savings: {e}")
            raise 