"""
Main application entry point.
"""

import os
import yaml
import logging.config
from typing import Dict, Any
from pyspark.sql import SparkSession

from datalake.core import DataLakeTable, MetricsCollector
from datalake.ingestion import DataIngestion
from datalake.optimization import QueryOptimizer
from datalake.governance import DataGovernance

from processors.image import ImageProcessor
from processors.metadata import MetadataProcessor
from processors.enrichment import DataEnrichment

from storage.raw import RawZone
from storage.processed import ProcessedZone
from storage.curated import CuratedZone

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logging(config_path: str) -> None:
    """Setup logging configuration."""
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create Spark session."""
    builder = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master'])
    
    # Add packages
    if 'packages' in config['spark']:
        builder = builder.config(
            'spark.jars.packages',
            ','.join(config['spark']['packages'])
        )
    
    return builder.getOrCreate()

def main():
    """Main application entry point."""
    # Load configuration
    config = load_config('config/settings.yaml')
    setup_logging('config/logging.yaml')
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Image Data Lake application")
    
    try:
        # Create Spark session
        spark = create_spark_session(config)
        
        # Initialize components
        metrics = MetricsCollector()
        
        raw = RawZone(spark, config['storage']['raw_path'])
        processed = ProcessedZone(spark, config['storage']['processed_path'])
        curated = CuratedZone(spark, config['storage']['curated_path'])
        
        ingestion = DataIngestion(spark, metrics)
        optimizer = QueryOptimizer(spark)
        governance = DataGovernance(spark)
        
        image_processor = ImageProcessor(spark)
        metadata_processor = MetadataProcessor(spark)
        enrichment = DataEnrichment(spark)
        
        # Process images
        logger.info("Processing images")
        
        # 1. Ingest raw images
        raw_data = ingestion.batch_ingest_images(
            config['storage']['raw_path'],
            batch_size=config['processing']['batch_size']
        )
        
        # 2. Process images
        processed_data = image_processor.resize_images(
            raw_data,
            (
                config['processing']['image']['resize']['width'],
                config['processing']['image']['resize']['height']
            )
        )
        
        # 3. Extract metadata
        processed_data = metadata_processor.extract_image_info(
            processed_data
        )
        
        # 4. Enrich data
        processed_data = enrichment.add_technical_metadata(
            processed_data
        )
        
        # 5. Write to processed zone
        processed.write_data(
            processed_data,
            "images"
        )
        
        # 6. Optimize storage
        optimizer.optimize_table(
            f"{config['storage']['processed_path']}/images",
            config['optimization']['z_order_columns']
        )
        
        # 7. Track lineage
        governance.track_lineage(
            f"{config['storage']['processed_path']}/images",
            [f"{config['storage']['raw_path']}/images"],
            "image_processing"
        )
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise
    
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 