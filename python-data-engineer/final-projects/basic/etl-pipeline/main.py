"""
Main entry point for ETL pipeline.
"""

import click
import logging
from src.etl import ETLPipeline
from src.utils.logging import setup_logger

logger = setup_logger(__name__)

@click.group()
def cli():
    """ETL Pipeline CLI"""
    pass

@cli.command()
@click.option('--mode', default='all', help='Pipeline mode: all, customers, transactions')
def run(mode):
    """Run ETL pipeline."""
    logger.info(f"Starting ETL pipeline in {mode} mode")
    
    pipeline = ETLPipeline()
    try:
        if mode in ['all', 'customers']:
            pipeline.process_customer_data()
        
        if mode in ['all', 'transactions']:
            pipeline.process_transaction_data()
            
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise
    finally:
        pipeline.close()

@cli.command()
def schedule():
    """Schedule ETL pipeline jobs."""
    logger.info("Starting ETL pipeline scheduler")
    
    pipeline = ETLPipeline()
    try:
        pipeline.schedule_jobs()
    except KeyboardInterrupt:
        logger.info("Stopping scheduler")
    finally:
        pipeline.close()

if __name__ == "__main__":
    cli() 