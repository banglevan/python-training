"""
Integration tests for data pipeline.
"""

import pytest
from datetime import datetime, timedelta
from src.utils.config import Config
from src.ingestion.producers.event_producer import EventProducer
from src.ingestion.consumers.event_consumer import EventConsumer
from src.processing.batch.historical_processor import HistoricalProcessor
from src.storage.delta.writer import DeltaWriter
from src.storage.elasticsearch.writer import ElasticsearchWriter
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Create Spark session."""
    return (
        SparkSession.builder
            .appName("IntegrationTests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .master("local[*]")
            .getOrCreate()
    )

@pytest.fixture
def config():
    """Test configuration."""
    return Config("tests/fixtures/config")

@pytest.fixture
def delta_writer(spark, config):
    """Delta writer instance."""
    return DeltaWriter(spark, config)

@pytest.fixture
def es_writer(config):
    """Elasticsearch writer instance."""
    return ElasticsearchWriter(config)

def test_end_to_end_pipeline(
    spark,
    config,
    delta_writer,
    es_writer
):
    """Test end-to-end data pipeline."""
    # Create producer and generate events
    producer = EventProducer(config)
    events = [producer.generate_event() for _ in range(100)]
    
    # Send events
    for event in events:
        producer.send_event(event)
    
    # Create and run consumer
    consumer = EventConsumer(config, delta_writer, es_writer)
    consumer.run(timeout=30)  # Run for 30 seconds
    
    # Process historical data
    processor = HistoricalProcessor(spark, config)
    start_date = datetime.now() - timedelta(days=1)
    end_date = datetime.now()
    
    metrics = processor.process_daily_metrics(start_date, end_date)
    
    # Verify metrics
    assert 'sales' in metrics
    assert 'users' in metrics
    assert 'products' in metrics
    
    # Verify data in Delta Lake
    sales_df = spark.read.format("delta").load(
        f"{config.storage.delta_path}/metrics_sales"
    )
    assert sales_df.count() > 0
    
    # Verify data in Elasticsearch
    es_client = config.get_elasticsearch_client()
    sales_count = es_client.count(index="metrics_sales")
    assert sales_count['count'] > 0 