"""
Streaming data ingestion using Spark Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from typing import Dict, Any, Optional
import logging
from datetime import datetime
from ..utils.validation import DataValidator
from ...storage.delta.delta_manager import DeltaManager

logger = logging.getLogger(__name__)

class StreamProcessor:
    """Process streaming data."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any]
    ):
        """
        Initialize stream processor.
        
        Args:
            spark: Spark session
            config: Processor configuration
        """
        self.spark = spark
        self.config = config
        self.validator = DataValidator()
        self.delta_manager = DeltaManager(spark)
    
    def process_stream(
        self,
        source_config: Dict[str, Any],
        target_table: str
    ) -> StreamingQuery:
        """
        Process streaming data.
        
        Args:
            source_config: Source configuration
            target_table: Target Delta table
            
        Returns:
            Streaming query
        """
        try:
            logger.info(f"Starting stream processing for {target_table}")
            
            # Create streaming DataFrame
            stream_df = self._create_stream(source_config)
            
            # Add processing logic
            processed_df = self._process_data(stream_df)
            
            # Write stream to Delta table
            query = (
                processed_df.writeStream
                .format('delta')
                .outputMode(self.config.get('output_mode', 'append'))
                .option(
                    'checkpointLocation',
                    f"{self.config['checkpoint_dir']}/{target_table}"
                )
                .table(target_table)
            )
            
            logger.info(f"Stream processing started for {target_table}")
            return query
            
        except Exception as e:
            logger.error(f"Stream processing failed: {e}")
            raise
    
    def _create_stream(
        self,
        source_config: Dict[str, Any]
    ) -> DataFrame:
        """Create streaming DataFrame."""
        try:
            format = source_config.get('format', 'kafka')
            options = source_config.get('options', {})
            
            return (
                self.spark.readStream
                .format(format)
                .options(**options)
                .load()
            )
            
        except Exception as e:
            logger.error(f"Failed to create stream: {e}")
            raise
    
    def _process_data(self, df: DataFrame) -> DataFrame:
        """Process streaming data."""
        try:
            # Add watermark for late data
            if 'watermark' in self.config:
                df = df.withWatermark(
                    self.config['watermark']['timestamp_column'],
                    self.config['watermark']['delay']
                )
            
            # Add metadata
            df = df.withColumn(
                'processing_timestamp',
                F.current_timestamp()
            )
            
            # Apply transformations
            for transform in self.config.get('transformations', []):
                df = self._apply_transformation(df, transform)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to process data: {e}")
            raise
    
    def _apply_transformation(
        self,
        df: DataFrame,
        transform: Dict[str, Any]
    ) -> DataFrame:
        """Apply single transformation."""
        try:
            transform_type = transform['type']
            
            if transform_type == 'select':
                return df.select(transform['columns'])
            elif transform_type == 'filter':
                return df.filter(transform['condition'])
            elif transform_type == 'groupBy':
                return df.groupBy(
                    transform['columns']
                ).agg(transform['aggregations'])
            else:
                logger.warning(f"Unknown transformation type: {transform_type}")
                return df
                
        except Exception as e:
            logger.error(f"Failed to apply transformation: {e}")
            raise 