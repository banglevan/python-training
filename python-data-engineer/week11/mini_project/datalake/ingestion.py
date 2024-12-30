"""
Data ingestion components for Data Lake Platform.
"""

from typing import Dict, Any, Optional, List, Generator
import logging
from datetime import datetime
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from .core import MetricsCollector
from .formats import ImageHandler, MetadataHandler

logger = logging.getLogger(__name__)

class DataIngestion:
    """Data ingestion management."""
    
    def __init__(
        self,
        spark: SparkSession,
        metrics: Optional[MetricsCollector] = None
    ):
        """Initialize ingestion manager."""
        self.spark = spark
        self.metrics = metrics or MetricsCollector()
        self.image_handler = ImageHandler()
        self.metadata_handler = MetadataHandler()
    
    def batch_ingest_images(
        self,
        source_path: str,
        target_path: str,
        batch_size: int = 1000,
        file_pattern: str = "*.{jpg,jpeg,png}"
    ) -> None:
        """
        Batch ingest images.
        
        Args:
            source_path: Source directory
            target_path: Target path
            batch_size: Batch size
            file_pattern: File pattern to match
        """
        try:
            self.metrics.start_operation("batch_ingest_images")
            
            # Create schema for image data
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("filename", StringType(), True),
                StructField("content", BinaryType(), True),
                StructField("mime_type", StringType(), True),
                StructField("size", LongType(), True),
                StructField("metadata", StructType([
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                    StructField("format", StringType(), True),
                    StructField("mode", StringType(), True)
                ]), True),
                StructField("ingestion_time", TimestampType(), True)
            ])
            
            # Process files in batches
            for batch in self._get_file_batches(source_path, file_pattern, batch_size):
                records = []
                
                for file_path in batch:
                    try:
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            
                        # Process image
                        result = self.image_handler.read(content)
                        
                        # Create record
                        record = {
                            'id': str(hash(file_path)),
                            'filename': os.path.basename(file_path),
                            'content': content,
                            'mime_type': f"image/{result['metadata']['format'].lower()}",
                            'size': result['metadata']['size'],
                            'metadata': result['metadata'],
                            'ingestion_time': datetime.now()
                        }
                        
                        records.append(record)
                        
                    except Exception as e:
                        logger.error(f"Failed to process {file_path}: {e}")
                
                if records:
                    # Create DataFrame
                    df = self.spark.createDataFrame(records, schema)
                    
                    # Write batch
                    df.write.format("delta") \
                        .mode("append") \
                        .save(target_path)
            
            self.metrics.end_operation("batch_ingest_images", True)
            
        except Exception as e:
            self.metrics.end_operation("batch_ingest_images", False)
            logger.error(f"Batch ingestion failed: {e}")
            raise
    
    def stream_ingest_images(
        self,
        source_path: str,
        target_path: str,
        checkpoint_path: str,
        trigger_interval: str = "1 minute"
    ) -> None:
        """
        Stream ingest images.
        
        Args:
            source_path: Source directory
            target_path: Target path
            checkpoint_path: Checkpoint location
            trigger_interval: Trigger interval
        """
        try:
            self.metrics.start_operation("stream_ingest_images")
            
            # Create streaming query
            query = self.spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "binaryFile") \
                .option("pathGlobFilter", "*.{jpg,jpeg,png}") \
                .load(source_path) \
                .writeStream \
                .format("delta") \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime=trigger_interval) \
                .start(target_path)
            
            # Wait for termination
            query.awaitTermination()
            
            self.metrics.end_operation("stream_ingest_images", True)
            
        except Exception as e:
            self.metrics.end_operation("stream_ingest_images", False)
            logger.error(f"Stream ingestion failed: {e}")
            raise
    
    def _get_file_batches(
        self,
        directory: str,
        pattern: str,
        batch_size: int
    ) -> Generator[List[str], None, None]:
        """Get batches of files."""
        batch = []
        
        for file_path in Path(directory).rglob(pattern):
            batch.append(str(file_path))
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        if batch:
            yield batch 