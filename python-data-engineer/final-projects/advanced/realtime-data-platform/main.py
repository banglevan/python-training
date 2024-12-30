"""
Main entry point for the real-time data platform.
"""

import click
from datetime import datetime, timedelta
import logging
from multiprocessing import Process
from src.utils.config import Config
from src.utils.logging import setup_logging
from src.ingestion.producers.event_producer import EventProducer
from src.ingestion.consumers.event_consumer import EventConsumer
from src.processing.batch.historical_processor import HistoricalProcessor
from src.monitoring.alerts.alert_manager import AlertManager
from src.storage.delta.writer import DeltaWriter
from src.storage.elasticsearch.writer import ElasticsearchWriter
from pyspark.sql import SparkSession

setup_logging()
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session."""
    return (
        SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )

@click.group()
def cli():
    """Real-time data platform CLI."""
    pass

@cli.command()
@click.option('--config-path', default='config', help='Path to config directory')
def start_producer(config_path):
    """Start event producer."""
    try:
        config = Config(config_path)
        producer = EventProducer(config)
        producer.run()
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise

@cli.command()
@click.option('--config-path', default='config', help='Path to config directory')
def start_consumer(config_path):
    """Start event consumer."""
    try:
        config = Config(config_path)
        spark = create_spark_session("EventConsumer")
        
        delta_writer = DeltaWriter(spark, config)
        es_writer = ElasticsearchWriter(config)
        
        consumer = EventConsumer(config, delta_writer, es_writer)
        consumer.run()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise

@cli.command()
@click.option('--config-path', default='config', help='Path to config directory')
@click.option('--days', default=1, help='Number of days to process')
def process_batch(config_path, days):
    """Run batch processing job."""
    try:
        config = Config(config_path)
        spark = create_spark_session("BatchProcessor")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        processor = HistoricalProcessor(spark, config)
        metrics = processor.process_daily_metrics(start_date, end_date)
        processor.save_metrics(metrics, end_date)
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        raise

@cli.command()
@click.option('--config-path', default='config', help='Path to config directory')
def start_monitoring(config_path):
    """Start monitoring and alerts."""
    try:
        config = Config(config_path)
        alert_manager = AlertManager(config)
        
        # Start monitoring loop
        while True:
            metrics = alert_manager.check_system_metrics()
            for alert in metrics:
                alert_manager.send_alert(alert)
            
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")
        raise

@cli.command()
@click.option('--config-path', default='config', help='Path to config directory')
def start_all(config_path):
    """Start all components."""
    try:
        # Start components in separate processes
        processes = []
        
        # Start producer
        producer_process = Process(
            target=start_producer,
            args=(config_path,)
        )
        processes.append(producer_process)
        
        # Start consumer
        consumer_process = Process(
            target=start_consumer,
            args=(config_path,)
        )
        processes.append(consumer_process)
        
        # Start monitoring
        monitoring_process = Process(
            target=start_monitoring,
            args=(config_path,)
        )
        processes.append(monitoring_process)
        
        # Start all processes
        for p in processes:
            p.start()
        
        # Wait for all processes
        for p in processes:
            p.join()
            
    except KeyboardInterrupt:
        logger.info("Stopping all processes...")
        for p in processes:
            p.terminate()
    except Exception as e:
        logger.error(f"Platform startup failed: {e}")
        raise

if __name__ == "__main__":
    cli() 