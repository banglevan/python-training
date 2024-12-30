"""
ETL orchestration module.
"""

import logging
from typing import Dict, Any
import click
from datetime import datetime, timedelta
import schedule
import time
from src.utils.config import Config
from src.utils.logging import setup_logger
from src.extractors.api import APIExtractor
from src.extractors.database import DatabaseExtractor
from src.extractors.file import FileExtractor
from src.transformers.cleaner import DataCleaner
from src.transformers.validator import DataValidator
from src.loaders.warehouse import WarehouseLoader

logger = setup_logger(__name__)

class ETLPipeline:
    """ETL pipeline orchestration."""
    
    def __init__(self):
        """Initialize pipeline."""
        self.config = Config()
        self._init_components()
    
    def _init_components(self):
        """Initialize pipeline components."""
        try:
            # Extractors
            self.api_extractor = APIExtractor(self.config)
            self.db_extractor = DatabaseExtractor(self.config)
            self.file_extractor = FileExtractor(self.config)
            
            # Transformers
            self.cleaner = DataCleaner(self.config)
            self.validator = DataValidator(self.config)
            
            # Loader
            self.loader = WarehouseLoader(self.config)
            
            logger.info("Initialized pipeline components")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            raise
    
    def process_customer_data(self):
        """Process customer data pipeline."""
        try:
            logger.info("Starting customer data pipeline")
            
            # Extract
            df = self.db_extractor.extract_customer_data()
            
            # Transform
            df = self.cleaner.clean_customer_data(df)
            if not self.validator.validate_customer_data(df):
                raise ValueError("Customer data validation failed")
            
            # Load
            self.loader.load_dimension(
                df,
                'dim_customers',
                ['customer_id']
            )
            
            logger.info("Completed customer data pipeline")
            
        except Exception as e:
            logger.error(f"Customer pipeline failed: {e}")
            raise
    
    def process_transaction_data(self):
        """Process transaction data pipeline."""
        try:
            logger.info("Starting transaction data pipeline")
            
            # Extract
            start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            df = self.db_extractor.extract_transaction_data(
                start_date,
                end_date
            )
            
            # Transform
            df = self.cleaner.clean_transaction_data(df)
            if not self.validator.validate_transaction_data(df):
                raise ValueError("Transaction data validation failed")
            
            # Load
            self.loader.load_fact(
                df,
                'fact_transactions'
            )
            
            logger.info("Completed transaction data pipeline")
            
        except Exception as e:
            logger.error(f"Transaction pipeline failed: {e}")
            raise
    
    def close(self):
        """Close pipeline components."""
        self.api_extractor.close()
        self.db_extractor.close()
        self.loader.close()

@click.group()
def cli():
    """ETL pipeline CLI."""
    pass

@cli.command()
def run_all():
    """Run all pipelines."""
    pipeline = ETLPipeline()
    try:
        pipeline.process_customer_data()
        pipeline.process_transaction_data()
    finally:
        pipeline.close()

@cli.command()
def schedule_jobs():
    """Schedule pipeline jobs."""
    pipeline = ETLPipeline()
    
    try:
        # Schedule customer pipeline
        schedule.every().day.at("00:00").do(pipeline.process_customer_data)
        
        # Schedule transaction pipeline
        schedule.every().hour.do(pipeline.process_transaction_data)
        
        logger.info("Started job scheduler")
        
        while True:
            schedule.run_pending()
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Stopping scheduler")
    finally:
        pipeline.close()

if __name__ == "__main__":
    cli() 