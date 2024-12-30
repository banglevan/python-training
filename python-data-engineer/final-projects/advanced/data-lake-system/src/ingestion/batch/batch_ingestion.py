"""
Batch data ingestion using Apache Spark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Dict, Any, Optional
import yaml
import logging
from datetime import datetime
from ..utils.validation import DataValidator
from ...storage.delta.delta_manager import DeltaManager

logger = logging.getLogger(__name__)

class BatchIngestionJob:
    """Batch data ingestion job."""
    
    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any]
    ):
        """
        Initialize batch ingestion job.
        
        Args:
            spark: Spark session
            config: Job configuration
        """
        self.spark = spark
        self.config = config
        self.validator = DataValidator()
        self.delta_manager = DeltaManager(spark)
        
    def ingest_data(
        self,
        source_path: str,
        target_table: str,
        schema: Optional[StructType] = None
    ) -> bool:
        """
        Ingest data from source to Delta table.
        
        Args:
            source_path: Path to source data
            target_table: Target Delta table name
            schema: Optional schema for data
            
        Returns:
            True if ingestion successful
        """
        try:
            logger.info(f"Starting ingestion for {source_path}")
            
            # Read source data
            df = self._read_source_data(source_path, schema)
            
            # Validate data
            validation_result = self.validator.validate_dataframe(
                df, self.config.get('validation_rules', {})
            )
            
            if not validation_result['passed']:
                logger.error(
                    f"Data validation failed: {validation_result['errors']}"
                )
                return False
            
            # Add metadata columns
            df = self._add_metadata(df)
            
            # Write to Delta table
            self.delta_manager.write_table(
                df,
                target_table,
                mode=self.config.get('write_mode', 'append'),
                partition_by=self.config.get('partition_columns', [])
            )
            
            logger.info(f"Successfully ingested data to {target_table}")
            return True
            
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
    
    def _read_source_data(
        self,
        source_path: str,
        schema: Optional[StructType] = None
    ) -> DataFrame:
        """Read data from source path."""
        try:
            format = self._detect_format(source_path)
            
            read_options = self.config.get('read_options', {})
            read_options['path'] = source_path
            
            if schema:
                read_options['schema'] = schema
            
            return (
                self.spark.read.format(format)
                .options(**read_options)
                .load()
            )
            
        except Exception as e:
            logger.error(f"Failed to read source data: {e}")
            raise
    
    def _detect_format(self, path: str) -> str:
        """Detect file format from path."""
        if path.endswith('.csv'):
            return 'csv'
        elif path.endswith('.json'):
            return 'json'
        elif path.endswith('.parquet'):
            return 'parquet'
        elif path.endswith('.xml'):
            return 'xml'
        else:
            return self.config.get('default_format', 'parquet')
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """Add metadata columns to DataFrame."""
        return df.withColumn(
            'ingestion_timestamp',
            F.current_timestamp()
        ).withColumn(
            'source_file',
            F.input_file_name()
        ) 